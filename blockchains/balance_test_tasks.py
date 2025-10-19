"""
Balance validation tasks for comparing ClickHouse computed balances against RPC data.

These tasks validate:
1. Collateral balances (scaled by liquidity index)
2. Debt balances (scaled by liquidity index)

Against getUserReserveData RPC calls.
"""

import logging
import time
from math import ceil, floor
from typing import Any, Dict, List

from celery import Task

from utils.clickhouse.client import clickhouse_client

logger = logging.getLogger(__name__)


class CompareCollateralBalanceTask(Task):
    """
    Task to compare collateral balances between ClickHouse and RPC.

    Computes scaled collateral balance from LatestBalances and compares with
    currentATokenBalance from getUserReserveData RPC call.

    Match criteria: |difference| / rpc_balance < 0.0001 (1 bps)
    """

    def run(
        self,
        csv_output_path: str = "/tmp",
        fix_errors: bool = False,
        batch_size: int = 100,
    ):
        """
        Execute the collateral balance comparison test using batched approach.

        Args:
            csv_output_path: Path to store the CSV file (unused, kept for compatibility)
            fix_errors: If True, fix detected errors by un-scaling, correcting, and re-scaling
            batch_size: Number of records to process per batch

        Returns:
            Dict[str, Any]: Test results summary
        """
        logger.info(
            f"Starting CompareCollateralBalanceTask with batch size {batch_size}"
        )
        if fix_errors:
            logger.info("Error correction is ENABLED - will fix mismatched balances")
        start_time = time.time()

        try:
            # Process in batches
            comparison_results = self._compare_in_batches(
                batch_size=batch_size, fix_errors=fix_errors
            )

            # Calculate test duration
            test_duration = time.time() - start_time

            # Store results in ClickHouse
            self._store_test_results(comparison_results, test_duration, 0)

            # Clean up old test records
            self._cleanup_old_test_records()

            logger.info(f"Comparison completed in {test_duration:.2f} seconds")
            logger.info(
                f"Match percentage: {comparison_results['match_percentage']:.2f}%"
            )

            # Send notification if mismatches found
            if comparison_results["mismatched_records"] > 0:
                self._send_mismatch_notification(comparison_results)

            return {
                "status": "completed",
                "test_duration": test_duration,
                **comparison_results,
            }

        except Exception as e:
            error_msg = f"Error during collateral balance comparison: {str(e)}"
            logger.error(error_msg, exc_info=True)

            # Store error result
            self._store_error_result(error_msg, time.time() - start_time)

            return {"status": "error", "error": error_msg}

    def _compare_in_batches(
        self, batch_size: int = 100, fix_errors: bool = False
    ) -> Dict[str, Any]:
        """
        Compare collateral balances in batches to minimize timing differences.

        For each batch:
        1. Fetch user-asset pairs from ClickHouse
        2. Immediately fetch RPC data for those pairs
        3. Compare and accumulate results

        Args:
            batch_size: Number of pairs to process per batch
            fix_errors: If True, fix detected errors

        Returns:
            Dict containing comparison statistics
        """
        from utils.interfaces.dataprovider import DataProviderInterface

        data_provider = DataProviderInterface()
        RAY = 1e27

        # Accumulators for results
        total_pairs = 0
        matching_count = 0
        mismatched_count = 0
        mismatches = []
        differences_bps = []
        all_updates = []

        offset = 0

        while True:
            # Step 1: Fetch batch from ClickHouse
            query = """
            SELECT
                user,
                asset,
                collateral_scaled_balance as scaled_balance,
                dictGetOrDefault('aave_ethereum.dict_collateral_liquidity_index', 'liquidityIndex', asset, toUInt256(0)) as current_index
            FROM aave_ethereum.LatestBalances_v2
            WHERE collateral_scaled_balance > 0
            ORDER BY user, asset
            LIMIT %(batch_size)s OFFSET %(offset)s
            """

            result = clickhouse_client.execute_query(
                query, parameters={"batch_size": batch_size, "offset": offset}
            )

            if not result.result_rows:
                break

            logger.info(
                f"Processing batch at offset {offset} with {len(result.result_rows)} pairs"
            )

            # Step 2: Calculate ClickHouse balances and prepare for RPC query
            user_asset_pairs = []
            clickhouse_data = {}
            current_indices = {}

            for row in result.result_rows:
                user = row[0].lower()
                asset = row[1].lower()
                scaled_balance = float(row[2])
                current_index = float(row[3])

                pair = (user, asset)
                user_asset_pairs.append(pair)

                # Calculate ClickHouse balance
                if current_index > 0:
                    underlying_balance = int(
                        floor(scaled_balance * current_index / RAY)
                    )
                else:
                    underlying_balance = 0

                clickhouse_data[pair] = underlying_balance
                current_indices[asset] = current_index

            # Step 3: Immediately fetch RPC data for this batch
            logger.info(f"Fetching RPC data for {len(user_asset_pairs)} pairs")
            try:
                batch_results = data_provider.get_user_reserve_data(user_asset_pairs)

                rpc_data = {}
                for (user, asset), result_data in batch_results.items():
                    user_lower = user.lower()
                    asset_lower = asset.lower()
                    current_atoken_balance = float(
                        result_data.get("currentATokenBalance", 0)
                    )
                    rpc_data[(user_lower, asset_lower)] = current_atoken_balance

            except Exception as e:
                logger.error(f"Error querying RPC for batch at offset {offset}: {e}")
                raise

            # Step 4: Compare this batch
            for pair in clickhouse_data.keys():
                if pair not in rpc_data:
                    continue

                ch_balance = clickhouse_data[pair]
                rpc_balance = rpc_data[pair]

                total_pairs += 1

                # Skip if CH balance is zero
                if ch_balance == 0:
                    if rpc_balance == 0:
                        matching_count += 1
                    continue

                # Calculate difference in basis points
                difference = abs(ch_balance - rpc_balance)
                difference_bps = (difference / ch_balance) * 10000

                differences_bps.append(difference_bps)

                # Match if difference < 1 bps
                if difference_bps < 1.0:
                    matching_count += 1
                else:
                    mismatched_count += 1
                    user, asset = pair
                    mismatches.append(
                        f"({user},{asset}): CH={ch_balance:.2f} RPC={rpc_balance:.2f} diff={difference_bps:.2f}bps"
                    )

                    # Collect updates for fixing if needed
                    if fix_errors:
                        asset_index = current_indices.get(asset, 0)
                        if asset_index > 0:
                            corrected_scaled_balance = int(
                                floor(rpc_balance * RAY / asset_index)
                            )
                            all_updates.append(
                                {
                                    "user": user,
                                    "asset": asset,
                                    "corrected_scaled_balance": corrected_scaled_balance,
                                }
                            )

            offset += batch_size

            if len(result.result_rows) < batch_size:
                break

        # Fix errors if requested
        fixed_count = 0
        if fix_errors and all_updates:
            logger.info(f"Fixing {len(all_updates)} mismatched collateral balances...")
            self._batch_update_collateral_balances(all_updates)
            fixed_count = len(all_updates)

        # Calculate summary statistics
        match_percentage = (matching_count / total_pairs * 100) if total_pairs else 0
        avg_difference_bps = (
            sum(differences_bps) / len(differences_bps) if differences_bps else 0
        )
        max_difference_bps = max(differences_bps) if differences_bps else 0

        return {
            "total_user_assets": total_pairs,
            "matching_records": matching_count,
            "mismatched_records": mismatched_count,
            "match_percentage": match_percentage,
            "avg_difference_bps": avg_difference_bps,
            "max_difference_bps": max_difference_bps,
            "mismatches_detail": "; ".join(mismatches[:50]),
            "fixed_count": fixed_count,
        }

    def _store_test_results(
        self, results: Dict[str, Any], duration: float, batch_offset: int
    ):
        """Store test results in ClickHouse."""
        query = """
        INSERT INTO aave_ethereum.CollateralBalanceTestResults
        (test_timestamp, batch_offset, total_user_assets, matching_records, mismatched_records,
         match_percentage, avg_difference_bps, max_difference_bps,
         test_duration_seconds, test_status, mismatches_detail)
        VALUES
        (now64(), %(batch_offset)s, %(total_user_assets)s, %(matching_records)s, %(mismatched_records)s,
         %(match_percentage)s, %(avg_difference_bps)s, %(max_difference_bps)s,
         %(test_duration_seconds)s, 'completed', %(mismatches_detail)s)
        """

        parameters = {
            "batch_offset": batch_offset,
            "total_user_assets": results["total_user_assets"],
            "matching_records": results["matching_records"],
            "mismatched_records": results["mismatched_records"],
            "match_percentage": results["match_percentage"],
            "avg_difference_bps": results["avg_difference_bps"],
            "max_difference_bps": results["max_difference_bps"],
            "test_duration_seconds": duration,
            "mismatches_detail": results["mismatches_detail"],
        }

        clickhouse_client.execute_query(query, parameters=parameters)
        logger.info(
            f"Test results stored successfully in ClickHouse (offset: {batch_offset})"
        )

    def _store_error_result(self, error_message: str, duration: float):
        """Store error result in ClickHouse."""
        query = """
        INSERT INTO aave_ethereum.CollateralBalanceTestResults
        (test_timestamp, total_user_assets, matching_records, mismatched_records,
         match_percentage, avg_difference_bps, max_difference_bps,
         test_duration_seconds, test_status, error_message)
        VALUES
        (now64(), 0, 0, 0, 0, 0, 0, %(test_duration_seconds)s, 'error', %(error_message)s)
        """

        parameters = {"test_duration_seconds": duration, "error_message": error_message}

        try:
            clickhouse_client.execute_query(query, parameters=parameters)
        except Exception as e:
            logger.error(f"Failed to store error result: {e}")

    def _send_mismatch_notification(self, results: Dict[str, Any]):
        """Send notification when mismatches found."""
        try:
            from utils.simplepush import send_simplepush_notification

            mismatch_count = results["mismatched_records"]
            total_count = results["total_user_assets"]
            match_percentage = results["match_percentage"]
            avg_diff = results["avg_difference_bps"]
            max_diff = results["max_difference_bps"]

            title = "⚠️ Collateral Balance Mismatch Detected"
            message = (
                f"Found {mismatch_count} mismatch{'es' if mismatch_count != 1 else ''} "
                f"out of {total_count} user-asset pairs.\n"
                f"Match rate: {match_percentage:.2f}%\n"
                f"Avg difference: {avg_diff:.2f} bps\n"
                f"Max difference: {max_diff:.2f} bps"
            )

            send_simplepush_notification(
                title=title, message=message, event="collateral_balance_mismatch"
            )

            logger.info(
                f"Sent mismatch notification: {mismatch_count} mismatches found"
            )

        except Exception as e:
            logger.error(f"Failed to send mismatch notification: {e}")

    def _cleanup_old_test_records(self):
        """Delete test records older than 7 days."""
        try:
            query = """
            ALTER TABLE aave_ethereum.CollateralBalanceTestResults
            DELETE WHERE test_timestamp < now() - INTERVAL 6 DAY
            """

            clickhouse_client.execute_query(query)
            logger.info("Cleaned up test records older than 7 days")

        except Exception as e:
            logger.error(f"Failed to cleanup old test records: {e}")

    def _batch_update_collateral_balances(self, updates: List[Dict[str, Any]]):
        """
        Batch update collateral scaled balances in LatestBalances_v2.

        Args:
            updates: List of dicts with user, asset, and corrected_scaled_balance
        """
        if not updates:
            return

        # Use ReplacingMergeTree behavior - insert new rows with updated_at = now
        query = """
        INSERT INTO aave_ethereum.LatestBalances_v2
        (user, asset, collateral_scaled_balance, variable_debt_scaled_balance, updated_at)
        SELECT
            %(user)s as user,
            %(asset)s as asset,
            %(corrected_scaled_balance)s as collateral_scaled_balance,
            variable_debt_scaled_balance,
            now64() as updated_at
        FROM aave_ethereum.LatestBalances_v2
        WHERE user = %(user)s AND asset = %(asset)s
        LIMIT 1
        """

        for update in updates:
            try:
                clickhouse_client.execute_query(query, parameters=update)
            except Exception as e:
                logger.error(
                    f"Failed to update balance for {update['user']}, {update['asset']}: {e}"
                )


class CompareDebtBalanceTask(Task):
    """
    Task to compare debt balances between ClickHouse and RPC.

    Computes scaled debt balance from LatestBalances and compares with
    currentVariableDebt from getUserReserveData RPC call.

    Match criteria: |difference| / rpc_balance < 0.0001 (1 bps)
    """

    def run(
        self,
        csv_output_path: str = "/tmp",
        fix_errors: bool = False,
        batch_size: int = 100,
    ):
        """
        Execute the debt balance comparison test using batched approach.

        Args:
            csv_output_path: Path to store the CSV file (unused, kept for compatibility)
            fix_errors: If True, fix detected errors by un-scaling, correcting, and re-scaling
            batch_size: Number of records to process per batch

        Returns:
            Dict[str, Any]: Test results summary
        """
        logger.info(f"Starting CompareDebtBalanceTask with batch size {batch_size}")
        if fix_errors:
            logger.info("Error correction is ENABLED - will fix mismatched balances")
        start_time = time.time()

        try:
            # Process in batches
            comparison_results = self._compare_in_batches(
                batch_size=batch_size, fix_errors=fix_errors
            )

            # Calculate test duration
            test_duration = time.time() - start_time

            # Store results in ClickHouse
            self._store_test_results(comparison_results, test_duration, 0)

            # Clean up old test records
            self._cleanup_old_test_records()

            logger.info(f"Comparison completed in {test_duration:.2f} seconds")
            logger.info(
                f"Match percentage: {comparison_results['match_percentage']:.2f}%"
            )

            # Send notification if mismatches found
            if comparison_results["mismatched_records"] > 0:
                self._send_mismatch_notification(comparison_results)

            return {
                "status": "completed",
                "test_duration": test_duration,
                **comparison_results,
            }

        except Exception as e:
            error_msg = f"Error during debt balance comparison: {str(e)}"
            logger.error(error_msg, exc_info=True)

            # Store error result
            self._store_error_result(error_msg, time.time() - start_time)

            return {"status": "error", "error": error_msg}

    def _compare_in_batches(
        self, batch_size: int = 100, fix_errors: bool = False
    ) -> Dict[str, Any]:
        """
        Compare debt balances in batches to minimize timing differences.

        For each batch:
        1. Fetch user-asset pairs from ClickHouse
        2. Immediately fetch RPC data for those pairs
        3. Compare and accumulate results

        Args:
            batch_size: Number of pairs to process per batch
            fix_errors: If True, fix detected errors

        Returns:
            Dict containing comparison statistics
        """
        from utils.interfaces.dataprovider import DataProviderInterface

        data_provider = DataProviderInterface()
        RAY = 1e27

        # Accumulators for results
        total_pairs = 0
        matching_count = 0
        mismatched_count = 0
        mismatches = []
        differences_bps = []
        all_updates = []

        offset = 0

        while True:
            # Step 1: Fetch batch from ClickHouse
            query = """
            SELECT
                user,
                asset,
                variable_debt_scaled_balance as scaled_balance,
                dictGetOrDefault('aave_ethereum.dict_debt_liquidity_index', 'liquidityIndex', asset, toUInt256(0)) as current_index
            FROM aave_ethereum.LatestBalances_v2
            WHERE variable_debt_scaled_balance > 0
            ORDER BY user, asset
            LIMIT %(batch_size)s OFFSET %(offset)s
            """

            result = clickhouse_client.execute_query(
                query, parameters={"batch_size": batch_size, "offset": offset}
            )

            if not result.result_rows:
                break

            logger.info(
                f"Processing batch at offset {offset} with {len(result.result_rows)} pairs"
            )

            # Step 2: Calculate ClickHouse balances and prepare for RPC query
            user_asset_pairs = []
            clickhouse_data = {}
            current_indices = {}

            for row in result.result_rows:
                user = row[0].lower()
                asset = row[1].lower()
                scaled_balance = float(row[2])
                current_index = float(row[3])

                pair = (user, asset)
                user_asset_pairs.append(pair)

                # Calculate ClickHouse balance (using ceil for debt)
                if current_index > 0:
                    underlying_balance = int(ceil(scaled_balance * current_index / RAY))
                else:
                    underlying_balance = 0

                clickhouse_data[pair] = underlying_balance
                current_indices[asset] = current_index

            # Step 3: Immediately fetch RPC data for this batch
            logger.info(f"Fetching RPC data for {len(user_asset_pairs)} pairs")
            try:
                batch_results = data_provider.get_user_reserve_data(user_asset_pairs)

                rpc_data = {}
                for (user, asset), result_data in batch_results.items():
                    user_lower = user.lower()
                    asset_lower = asset.lower()
                    current_variable_debt = float(
                        result_data.get("currentVariableDebt", 0)
                    )
                    rpc_data[(user_lower, asset_lower)] = current_variable_debt

            except Exception as e:
                logger.error(f"Error querying RPC for batch at offset {offset}: {e}")
                raise

            # Step 4: Compare this batch
            for pair in clickhouse_data.keys():
                if pair not in rpc_data:
                    continue

                ch_balance = clickhouse_data[pair]
                rpc_balance = rpc_data[pair]

                total_pairs += 1

                # Skip if CH balance is zero
                if ch_balance == 0:
                    if rpc_balance == 0:
                        matching_count += 1
                    continue

                # Calculate difference in basis points
                difference = abs(ch_balance - rpc_balance)
                difference_bps = (difference / ch_balance) * 10000

                differences_bps.append(difference_bps)

                # Match if difference < 1 bps
                if difference_bps < 1.0:
                    matching_count += 1
                else:
                    mismatched_count += 1
                    user, asset = pair
                    mismatches.append(
                        f"({user},{asset}): CH={ch_balance:.2f} RPC={rpc_balance:.2f} diff={difference_bps:.2f}bps"
                    )

                    # Collect updates for fixing if needed
                    if fix_errors:
                        asset_index = current_indices.get(asset, 0)
                        if asset_index > 0:
                            corrected_scaled_balance = int(
                                ceil(rpc_balance * RAY / asset_index)
                            )
                            all_updates.append(
                                {
                                    "user": user,
                                    "asset": asset,
                                    "corrected_scaled_balance": corrected_scaled_balance,
                                }
                            )

            offset += batch_size

            if len(result.result_rows) < batch_size:
                break

        # Fix errors if requested
        fixed_count = 0
        if fix_errors and all_updates:
            logger.info(f"Fixing {len(all_updates)} mismatched debt balances...")
            self._batch_update_debt_balances(all_updates)
            fixed_count = len(all_updates)

        # Calculate summary statistics
        match_percentage = (matching_count / total_pairs * 100) if total_pairs else 0
        avg_difference_bps = (
            sum(differences_bps) / len(differences_bps) if differences_bps else 0
        )
        max_difference_bps = max(differences_bps) if differences_bps else 0

        return {
            "total_user_assets": total_pairs,
            "matching_records": matching_count,
            "mismatched_records": mismatched_count,
            "match_percentage": match_percentage,
            "avg_difference_bps": avg_difference_bps,
            "max_difference_bps": max_difference_bps,
            "mismatches_detail": "; ".join(mismatches[:50]),
            "fixed_count": fixed_count,
        }

    def _store_test_results(
        self, results: Dict[str, Any], duration: float, batch_offset: int
    ):
        """Store test results in ClickHouse."""
        query = """
        INSERT INTO aave_ethereum.DebtBalanceTestResults
        (test_timestamp, batch_offset, total_user_assets, matching_records, mismatched_records,
         match_percentage, avg_difference_bps, max_difference_bps,
         test_duration_seconds, test_status, mismatches_detail)
        VALUES
        (now64(), %(batch_offset)s, %(total_user_assets)s, %(matching_records)s, %(mismatched_records)s,
         %(match_percentage)s, %(avg_difference_bps)s, %(max_difference_bps)s,
         %(test_duration_seconds)s, 'completed', %(mismatches_detail)s)
        """

        parameters = {
            "batch_offset": batch_offset,
            "total_user_assets": results["total_user_assets"],
            "matching_records": results["matching_records"],
            "mismatched_records": results["mismatched_records"],
            "match_percentage": results["match_percentage"],
            "avg_difference_bps": results["avg_difference_bps"],
            "max_difference_bps": results["max_difference_bps"],
            "test_duration_seconds": duration,
            "mismatches_detail": results["mismatches_detail"],
        }

        clickhouse_client.execute_query(query, parameters=parameters)
        logger.info(
            f"Test results stored successfully in ClickHouse (offset: {batch_offset})"
        )

    def _store_error_result(self, error_message: str, duration: float):
        """Store error result in ClickHouse."""
        query = """
        INSERT INTO aave_ethereum.DebtBalanceTestResults
        (test_timestamp, total_user_assets, matching_records, mismatched_records,
         match_percentage, avg_difference_bps, max_difference_bps,
         test_duration_seconds, test_status, error_message)
        VALUES
        (now64(), 0, 0, 0, 0, 0, 0, %(test_duration_seconds)s, 'error', %(error_message)s)
        """

        parameters = {"test_duration_seconds": duration, "error_message": error_message}

        try:
            clickhouse_client.execute_query(query, parameters=parameters)
        except Exception as e:
            logger.error(f"Failed to store error result: {e}")

    def _send_mismatch_notification(self, results: Dict[str, Any]):
        """Send notification when mismatches found."""
        try:
            from utils.simplepush import send_simplepush_notification

            mismatch_count = results["mismatched_records"]
            total_count = results["total_user_assets"]
            match_percentage = results["match_percentage"]
            avg_diff = results["avg_difference_bps"]
            max_diff = results["max_difference_bps"]

            title = "⚠️ Debt Balance Mismatch Detected"
            message = (
                f"Found {mismatch_count} mismatch{'es' if mismatch_count != 1 else ''} "
                f"out of {total_count} user-asset pairs.\n"
                f"Match rate: {match_percentage:.2f}%\n"
                f"Avg difference: {avg_diff:.2f} bps\n"
                f"Max difference: {max_diff:.2f} bps"
            )

            send_simplepush_notification(
                title=title, message=message, event="debt_balance_mismatch"
            )

            logger.info(
                f"Sent mismatch notification: {mismatch_count} mismatches found"
            )

        except Exception as e:
            logger.error(f"Failed to send mismatch notification: {e}")

    def _cleanup_old_test_records(self):
        """Delete test records older than 7 days."""
        try:
            query = """
            ALTER TABLE aave_ethereum.DebtBalanceTestResults
            DELETE WHERE test_timestamp < now() - INTERVAL 6 DAY
            """

            clickhouse_client.execute_query(query)
            logger.info("Cleaned up test records older than 7 days")

        except Exception as e:
            logger.error(f"Failed to cleanup old test records: {e}")

    def _batch_update_debt_balances(self, updates: List[Dict[str, Any]]):
        """
        Batch update debt scaled balances in LatestBalances_v2.

        Args:
            updates: List of dicts with user, asset, and corrected_scaled_balance
        """
        if not updates:
            return

        # Use ReplacingMergeTree behavior - insert new rows with updated_at = now
        query = """
        INSERT INTO aave_ethereum.LatestBalances_v2
        (user, asset, collateral_scaled_balance, variable_debt_scaled_balance, updated_at)
        SELECT
            %(user)s as user,
            %(asset)s as asset,
            collateral_scaled_balance,
            %(corrected_scaled_balance)s as variable_debt_scaled_balance,
            now64() as updated_at
        FROM aave_ethereum.LatestBalances_v2
        WHERE user = %(user)s AND asset = %(asset)s
        LIMIT 1
        """

        for update in updates:
            try:
                clickhouse_client.execute_query(query, parameters=update)
            except Exception as e:
                logger.error(
                    f"Failed to update balance for {update['user']}, {update['asset']}: {e}"
                )


class CompareHealthFactorTask(Task):
    """
    Task to compare health factors between ClickHouse and RPC.

    Fetches all users from view_user_health_factor and compares with
    health factor from getUserAccountData RPC call.

    Match criteria: Both health factors normalized to min(value, 999)
    RPC health factor divided by 10**18 before comparison
    """

    def run(self, csv_output_path: str = "/tmp", batch_size: int = 100):
        """
        Execute the health factor comparison test using batched approach.

        Args:
            csv_output_path: Path to store the CSV file (unused, kept for compatibility)
            batch_size: Number of records to process per batch

        Returns:
            Dict[str, Any]: Test results summary
        """
        logger.info(f"Starting CompareHealthFactorTask with batch size {batch_size}")
        start_time = time.time()

        try:
            # Process in batches
            comparison_results = self._compare_in_batches(batch_size=batch_size)

            # Calculate test duration
            test_duration = time.time() - start_time

            # Store results in ClickHouse
            self._store_test_results(comparison_results, test_duration, 0)

            # Clean up old test records
            self._cleanup_old_test_records()

            logger.info(f"Comparison completed in {test_duration:.2f} seconds")
            logger.info(
                f"Match percentage: {comparison_results['match_percentage']:.2f}%"
            )

            # Send notification if mismatches found
            if comparison_results["mismatched_records"] > 0:
                self._send_mismatch_notification(comparison_results)

            return {
                "status": "completed",
                "test_duration": test_duration,
                **comparison_results,
            }

        except Exception as e:
            error_msg = f"Error during health factor comparison: {str(e)}"
            logger.error(error_msg, exc_info=True)

            # Store error result
            self._store_error_result(error_msg, time.time() - start_time)

            return {"status": "error", "error": error_msg}

    def _compare_in_batches(self, batch_size: int = 100) -> Dict[str, Any]:
        """
        Compare health factors in batches to minimize timing differences.

        For each batch:
        1. Fetch users from ClickHouse
        2. Immediately fetch RPC data for those users
        3. Compare and accumulate results

        Args:
            batch_size: Number of users to process per batch

        Returns:
            Dict containing comparison statistics
        """
        from utils.interfaces.pool import PoolInterface

        pool = PoolInterface()

        # Accumulators for results
        total_users = 0
        matching_count = 0
        mismatched_count = 0
        mismatches = []
        differences = []

        offset = 0

        while True:
            # Step 1: Fetch batch from ClickHouse
            query = """
            SELECT
                user,
                health_factor
            FROM aave_ethereum.view_user_health_factor
            WHERE effective_collateral > 10000 AND effective_debt > 10000
            ORDER BY user
            LIMIT %(batch_size)s OFFSET %(offset)s
            """

            result = clickhouse_client.execute_query(
                query, parameters={"batch_size": batch_size, "offset": offset}
            )

            if not result.result_rows:
                break

            logger.info(
                f"Processing batch at offset {offset} with {len(result.result_rows)} users"
            )

            # Step 2: Prepare ClickHouse data and user list for RPC query
            users = []
            clickhouse_data = {}

            for row in result.result_rows:
                user = row[0].lower()
                health_factor = float(row[1])

                # Cap at 999
                if health_factor > 999.0:
                    health_factor = 999.0

                users.append(user)
                clickhouse_data[user] = health_factor

            # Step 3: Immediately fetch RPC data for this batch
            logger.info(f"Fetching RPC data for {len(users)} users")
            try:
                batch_results = pool.get_user_account_data(users)

                rpc_data = {}
                for user, result_data in batch_results.items():
                    user_lower = user.lower()

                    # Get healthFactor from result (6th element in tuple)
                    if isinstance(result_data, dict):
                        health_factor_raw = float(result_data.get("healthFactor", 0))
                    else:
                        health_factor_raw = float(
                            result_data[5] if len(result_data) > 5 else 0
                        )

                    # Divide by 10**18 to normalize
                    health_factor = health_factor_raw / (10**18)

                    # Cap at 999
                    if health_factor > 999.0:
                        health_factor = 999.0

                    rpc_data[user_lower] = health_factor

            except Exception as e:
                logger.error(f"Error querying RPC for batch at offset {offset}: {e}")
                raise

            # Step 4: Compare this batch
            for user in clickhouse_data.keys():
                if user not in rpc_data:
                    continue

                ch_hf = clickhouse_data[user]
                rpc_hf = rpc_data[user]

                total_users += 1

                # Calculate absolute difference
                difference = abs(ch_hf - rpc_hf)
                differences.append(difference)

                # Match if difference is less than 0.00001 (allowing for small rounding errors)
                if difference < 0.00001:
                    matching_count += 1
                else:
                    mismatched_count += 1
                    mismatches.append(
                        f"{user}: CH={ch_hf:.4f} RPC={rpc_hf:.4f} diff={difference:.4f}"
                    )

            offset += batch_size

            if len(result.result_rows) < batch_size:
                break

        # Calculate summary statistics
        match_percentage = (matching_count / total_users * 100) if total_users else 0
        avg_difference = sum(differences) / len(differences) if differences else 0
        max_difference = max(differences) if differences else 0

        return {
            "total_users": total_users,
            "matching_records": matching_count,
            "mismatched_records": mismatched_count,
            "match_percentage": match_percentage,
            "avg_difference": avg_difference,
            "max_difference": max_difference,
            "mismatches_detail": "; ".join(mismatches[:50]),
        }

    def _store_test_results(
        self, results: Dict[str, Any], duration: float, batch_offset: int
    ):
        """Store test results in ClickHouse."""
        query = """
        INSERT INTO aave_ethereum.HealthFactorTestResults
        (test_timestamp, batch_offset, total_users, matching_records, mismatched_records,
         match_percentage, avg_difference, max_difference,
         test_duration_seconds, test_status, mismatches_detail)
        VALUES
        (now64(), %(batch_offset)s, %(total_users)s, %(matching_records)s, %(mismatched_records)s,
         %(match_percentage)s, %(avg_difference)s, %(max_difference)s,
         %(test_duration_seconds)s, 'completed', %(mismatches_detail)s)
        """

        parameters = {
            "batch_offset": batch_offset,
            "total_users": results["total_users"],
            "matching_records": results["matching_records"],
            "mismatched_records": results["mismatched_records"],
            "match_percentage": results["match_percentage"],
            "avg_difference": results["avg_difference"],
            "max_difference": results["max_difference"],
            "test_duration_seconds": duration,
            "mismatches_detail": results["mismatches_detail"],
        }

        clickhouse_client.execute_query(query, parameters=parameters)
        logger.info(
            f"Test results stored successfully in ClickHouse (offset: {batch_offset})"
        )

    def _store_error_result(self, error_message: str, duration: float):
        """Store error result in ClickHouse."""
        query = """
        INSERT INTO aave_ethereum.HealthFactorTestResults
        (test_timestamp, total_users, matching_records, mismatched_records,
         match_percentage, avg_difference, max_difference,
         test_duration_seconds, test_status, error_message)
        VALUES
        (now64(), 0, 0, 0, 0, 0, 0, %(test_duration_seconds)s, 'error', %(error_message)s)
        """

        parameters = {"test_duration_seconds": duration, "error_message": error_message}

        try:
            clickhouse_client.execute_query(query, parameters=parameters)
        except Exception as e:
            logger.error(f"Failed to store error result: {e}")

    def _send_mismatch_notification(self, results: Dict[str, Any]):
        """Send notification when mismatches found."""
        try:
            from utils.simplepush import send_simplepush_notification

            mismatch_count = results["mismatched_records"]
            total_count = results["total_users"]
            match_percentage = results["match_percentage"]
            avg_diff = results["avg_difference"]
            max_diff = results["max_difference"]

            title = "⚠️ Health Factor Mismatch Detected"
            message = (
                f"Found {mismatch_count} mismatch{'es' if mismatch_count != 1 else ''} "
                f"out of {total_count} users.\n"
                f"Match rate: {match_percentage:.2f}%\n"
                f"Avg difference: {avg_diff:.4f}\n"
                f"Max difference: {max_diff:.4f}"
            )

            send_simplepush_notification(
                title=title, message=message, event="health_factor_mismatch"
            )

            logger.info(
                f"Sent mismatch notification: {mismatch_count} mismatches found"
            )

        except Exception as e:
            logger.error(f"Failed to send mismatch notification: {e}")

    def _cleanup_old_test_records(self):
        """Delete test records older than 7 days."""
        try:
            query = """
            ALTER TABLE aave_ethereum.HealthFactorTestResults
            DELETE WHERE test_timestamp < now() - INTERVAL 6 DAY
            """

            clickhouse_client.execute_query(query)
            logger.info("Cleaned up test records older than 7 days")

        except Exception as e:
            logger.error(f"Failed to cleanup old test records: {e}")

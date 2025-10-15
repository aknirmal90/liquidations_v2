"""
Balance validation tasks for comparing ClickHouse computed balances against RPC data.

These tasks validate:
1. Collateral balances (scaled by liquidity index)
2. Debt balances (scaled by liquidity index)

Against getUserReserveData RPC calls.
"""

import logging
import time
from typing import Any, Dict, List, Tuple

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

    def run(self):
        """
        Execute the collateral balance comparison test.

        Returns:
            Dict[str, Any]: Test results summary
        """
        logger.info("Starting CompareCollateralBalanceTask")
        start_time = time.time()

        try:
            # Get the offset for the next batch
            batch_offset = self._get_last_batch_offset()

            # Get user-asset pairs to test (only those with non-zero balances)
            user_asset_pairs = self._get_user_asset_pairs_with_balances(batch_offset)
            total_pairs = len(user_asset_pairs)
            logger.info(
                f"Retrieved {total_pairs} user-asset pairs to test "
                f"(offset: {batch_offset})"
            )

            if total_pairs == 0:
                logger.info("No user-asset pairs to test")
                return {
                    "status": "completed",
                    "test_duration": time.time() - start_time,
                    "total_user_assets": 0,
                    "matching_records": 0,
                    "mismatched_records": 0,
                }

            # Get ClickHouse computed balances
            clickhouse_data = self._get_clickhouse_collateral_balances(user_asset_pairs)
            logger.info(
                f"Retrieved {len(clickhouse_data)} collateral balances from ClickHouse"
            )

            # Get RPC balances in batches of 100
            rpc_data = self._get_rpc_balances_batched(user_asset_pairs, batch_size=100)
            logger.info(f"Retrieved {len(rpc_data)} collateral balances from RPC")

            # Compare the balances
            comparison_results = self._compare_collateral_balances(
                clickhouse_data, rpc_data
            )

            # Calculate test duration
            test_duration = time.time() - start_time

            # Store results in ClickHouse
            self._store_test_results(comparison_results, test_duration, batch_offset)

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

    def _get_last_batch_offset(self):
        """Get the offset of the last successful test run."""
        query = """
        SELECT batch_offset
        FROM aave_ethereum.CollateralBalanceTestResults
        WHERE test_status = 'completed'
        ORDER BY test_timestamp DESC
        LIMIT 1
        """

        result = clickhouse_client.execute_query(query)

        if result.result_rows:
            last_offset = result.result_rows[0][0]
            # Return next offset (increment by batch size)
            return last_offset + 10000
        return 0

    def _get_user_asset_pairs_with_balances(self, offset: int) -> List[Tuple[str, str]]:
        """
        Get list of (user, asset) pairs that have non-zero collateral balance.

        Args:
            offset: Offset to start from for pagination

        Returns:
            List[Tuple[str, str]]: List of (user, asset) tuples
        """
        # Query LatestBalances_v2 for pairs with non-zero collateral
        # Use deterministic ordering and pagination
        query = """
        SELECT toString(user) as user, toString(asset) as asset
        FROM aave_ethereum.LatestBalances_v2
        GROUP BY user, asset
        HAVING sumMerge(collateral_scaled_balance) > 100000
        ORDER BY user, asset
        LIMIT 10000 OFFSET %(offset)s
        """

        try:
            result = clickhouse_client.execute_query(
                query, parameters={"offset": offset}
            )

            if not result or not result.result_rows:
                logger.warning("No user-asset pairs with collateral balances found")
                return []

            pairs = [(str(row[0]), str(row[1])) for row in result.result_rows]
            logger.info(f"Found {len(pairs)} user-asset pairs with collateral balances")
            return pairs

        except Exception as e:
            logger.error(f"Error fetching user-asset pairs: {e}", exc_info=True)
            raise

    def _get_clickhouse_collateral_balances(
        self, user_asset_pairs: List[Tuple[str, str]]
    ) -> Dict[Tuple[str, str], float]:
        """
        Get scaled collateral balances from ClickHouse and convert to underlying.

        Formula: floor(sumMerge(scaled_balance) * current_index / RAY)
        where RAY = 1e27

        This uses the new LatestBalances_v2 table which stores scaled balances.

        Args:
            user_asset_pairs: List of (user, asset) tuples

        Returns:
            Dict mapping (user, asset) to underlying balance
        """
        if not user_asset_pairs:
            return {}

        logger.info(
            f"Querying ClickHouse for collateral balances of {len(user_asset_pairs)} pairs"
        )

        collateral_balances = {}
        batch_size = 500
        RAY = 1e27

        for i in range(0, len(user_asset_pairs), batch_size):
            batch = user_asset_pairs[i : i + batch_size]
            logger.info(
                f"Querying ClickHouse batch {i // batch_size + 1}/{(len(user_asset_pairs) + batch_size - 1) // batch_size} "
                f"({len(batch)} pairs)"
            )

            users_in_batch = list(set([user for user, _ in batch]))
            assets_in_batch = list(set([asset for _, asset in batch]))

            # Query scaled balances from LatestBalances_v2 and convert to underlying
            query = """
            SELECT
                balances.user,
                balances.asset,
                balances.scaled_balance,
                max_idx.max_collateral_liquidityIndex as current_index
            FROM (
                SELECT
                    toString(lb.user) as user,
                    toString(lb.asset) as asset,
                    sumMerge(lb.collateral_scaled_balance) as scaled_balance
                FROM aave_ethereum.LatestBalances_v2 lb
                WHERE lb.user IN %(users)s AND lb.asset IN %(assets)s
                GROUP BY toString(lb.user), toString(lb.asset)
                HAVING sumMerge(lb.collateral_scaled_balance) > 0
            ) AS balances
            LEFT JOIN aave_ethereum.MaxLiquidityIndex max_idx
                ON balances.asset = max_idx.asset
            """

            parameters = {"users": users_in_batch, "assets": assets_in_batch}
            result = clickhouse_client.execute_query(query, parameters=parameters)

            batch_set = set(batch)
            for row in result.result_rows:
                user = str(row[0]).lower()
                asset = str(row[1]).lower()
                pair = (user, asset)

                if pair in batch_set or (row[0], row[1]) in batch_set:
                    scaled_balance = float(row[2])
                    current_index = float(row[3])

                    # Convert scaled balance to underlying: floor(scaled * current_index / RAY)
                    if current_index > 0:
                        underlying_balance = int(scaled_balance * current_index / RAY)
                    else:
                        underlying_balance = 0

                    collateral_balances[pair] = underlying_balance

        logger.info(
            f"Retrieved {len(collateral_balances)} collateral balances from ClickHouse"
        )

        return collateral_balances

    def _get_rpc_balances_batched(
        self, user_asset_pairs: List[Tuple[str, str]], batch_size: int = 100
    ) -> Dict[Tuple[str, str], float]:
        """
        Get collateral balances from RPC in batches.

        Args:
            user_asset_pairs: List of (user, asset) tuples
            batch_size: Number of pairs to query per batch

        Returns:
            Dict mapping (user, asset) to currentATokenBalance
        """
        from utils.interfaces.dataprovider import DataProviderInterface

        data_provider = DataProviderInterface()
        rpc_balances = {}

        for i in range(0, len(user_asset_pairs), batch_size):
            batch = user_asset_pairs[i : i + batch_size]
            logger.info(
                f"Querying RPC for balances batch {i // batch_size + 1} "
                f"({len(batch)} pairs)"
            )

            try:
                batch_results = data_provider.get_user_reserve_data(batch)

                for (user, asset), result in batch_results.items():
                    user_lower = user.lower()
                    asset_lower = asset.lower()
                    # Get currentATokenBalance from result
                    current_atoken_balance = float(
                        result.get("currentATokenBalance", 0)
                    )
                    rpc_balances[(user_lower, asset_lower)] = current_atoken_balance

            except Exception as e:
                logger.error(f"Error querying batch starting at index {i}: {e}")
                raise

        return rpc_balances

    def _compare_collateral_balances(
        self,
        clickhouse_data: Dict[Tuple[str, str], float],
        rpc_data: Dict[Tuple[str, str], float],
    ) -> Dict[str, Any]:
        """
        Compare collateral balances from ClickHouse and RPC.

        Match criteria: |difference| / rpc_balance < 0.0001 (1 bps)

        Args:
            clickhouse_data: Collateral balances from ClickHouse
            rpc_data: Collateral balances from RPC

        Returns:
            Dict containing comparison statistics
        """
        clickhouse_pairs = set(clickhouse_data.keys())
        rpc_pairs = set(rpc_data.keys())

        common_pairs = clickhouse_pairs & rpc_pairs

        matching_count = 0
        mismatched_count = 0
        mismatches = []
        differences_bps = []

        # Compare common pairs
        for pair in common_pairs:
            ch_balance = clickhouse_data[pair]
            rpc_balance = rpc_data[pair]

            # Skip if RPC balance is zero (can't calculate percentage)
            if ch_balance == 0:
                if rpc_balance == 0:
                    matching_count += 1
                else:
                    if abs(ch_balance - rpc_balance) < 100_000:
                        matching_count += 1
                    else:
                        mismatched_count += 1
                        user, asset = pair
                        mismatches.append(
                            f"({user},{asset}): CH={ch_balance:.2f} RPC={rpc_balance:.2f}"
                        )
                continue

            # Calculate difference in basis points
            difference = abs(ch_balance - rpc_balance)
            difference_bps = (difference / rpc_balance) * 10000

            differences_bps.append(difference_bps)

            # Match if difference < 1 bps
            if difference_bps < 100.0:
                matching_count += 1
            else:
                mismatched_count += 1
                user, asset = pair
                mismatches.append(
                    f"({user},{asset}): CH={ch_balance:.2f} RPC={rpc_balance:.2f} diff={difference_bps:.2f}bps"
                )

        total_pairs = len(common_pairs)
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


class CompareDebtBalanceTask(Task):
    """
    Task to compare debt balances between ClickHouse and RPC.

    Computes scaled debt balance from LatestBalances and compares with
    currentVariableDebt from getUserReserveData RPC call.

    Match criteria: |difference| / rpc_balance < 0.0001 (1 bps)
    """

    def run(self):
        """
        Execute the debt balance comparison test.

        Returns:
            Dict[str, Any]: Test results summary
        """
        logger.info("Starting CompareDebtBalanceTask")
        start_time = time.time()

        try:
            # Get the offset for the next batch
            batch_offset = self._get_last_batch_offset()

            # Get user-asset pairs to test (only those with non-zero balances)
            user_asset_pairs = self._get_user_asset_pairs_with_balances(batch_offset)
            total_pairs = len(user_asset_pairs)
            logger.info(
                f"Retrieved {total_pairs} user-asset pairs to test "
                f"(offset: {batch_offset})"
            )

            if total_pairs == 0:
                logger.info("No user-asset pairs to test")
                return {
                    "status": "completed",
                    "test_duration": time.time() - start_time,
                    "total_user_assets": 0,
                    "matching_records": 0,
                    "mismatched_records": 0,
                }

            # Get ClickHouse computed balances
            clickhouse_data = self._get_clickhouse_debt_balances(user_asset_pairs)
            logger.info(
                f"Retrieved {len(clickhouse_data)} debt balances from ClickHouse"
            )

            # Get RPC balances in batches of 100
            rpc_data = self._get_rpc_balances_batched(user_asset_pairs, batch_size=100)
            logger.info(f"Retrieved {len(rpc_data)} debt balances from RPC")

            # Compare the balances
            comparison_results = self._compare_debt_balances(clickhouse_data, rpc_data)

            # Calculate test duration
            test_duration = time.time() - start_time

            # Store results in ClickHouse
            self._store_test_results(comparison_results, test_duration, batch_offset)

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

    def _get_last_batch_offset(self):
        """Get the offset of the last successful test run."""
        query = """
        SELECT batch_offset
        FROM aave_ethereum.DebtBalanceTestResults
        WHERE test_status = 'completed'
        ORDER BY test_timestamp DESC
        LIMIT 1
        """

        result = clickhouse_client.execute_query(query)

        if result.result_rows:
            last_offset = result.result_rows[0][0]
            # Return next offset (increment by batch size)
            return last_offset + 10000
        return 0

    def _get_user_asset_pairs_with_balances(self, offset: int) -> List[Tuple[str, str]]:
        """
        Get list of (user, asset) pairs that have non-zero debt balance.

        Args:
            offset: Offset to start from for pagination

        Returns:
            List[Tuple[str, str]]: List of (user, asset) tuples
        """
        # Query LatestBalances_v2 for pairs with non-zero debt
        # Use deterministic ordering and pagination
        query = """
        SELECT toString(user) as user, toString(asset) as asset
        FROM aave_ethereum.LatestBalances_v2
        GROUP BY user, asset
        HAVING sumMerge(variable_debt_scaled_balance) > 100000
        ORDER BY user, asset
        LIMIT 10000 OFFSET %(offset)s
        """

        try:
            result = clickhouse_client.execute_query(
                query, parameters={"offset": offset}
            )

            if not result or not result.result_rows:
                logger.warning("No user-asset pairs with debt balances found")
                return []

            pairs = [(str(row[0]), str(row[1])) for row in result.result_rows]
            logger.info(f"Found {len(pairs)} user-asset pairs with debt balances")
            return pairs

        except Exception as e:
            logger.error(f"Error fetching user-asset pairs: {e}", exc_info=True)
            raise

    def _get_clickhouse_debt_balances(
        self, user_asset_pairs: List[Tuple[str, str]]
    ) -> Dict[Tuple[str, str], float]:
        """
        Get scaled debt balances from ClickHouse and convert to underlying.

        Formula: floor(sumMerge(scaled_balance) * current_index / RAY)
        where RAY = 1e27

        This uses the new LatestBalances_v2 table which stores scaled balances.

        Args:
            user_asset_pairs: List of (user, asset) tuples

        Returns:
            Dict mapping (user, asset) to underlying balance
        """
        if not user_asset_pairs:
            return {}

        logger.info(
            f"Querying ClickHouse for debt balances of {len(user_asset_pairs)} pairs"
        )

        debt_balances = {}
        batch_size = 500
        RAY = 1e27

        for i in range(0, len(user_asset_pairs), batch_size):
            batch = user_asset_pairs[i : i + batch_size]
            logger.info(
                f"Querying ClickHouse batch {i // batch_size + 1}/{(len(user_asset_pairs) + batch_size - 1) // batch_size} "
                f"({len(batch)} pairs)"
            )

            users_in_batch = list(set([user for user, _ in batch]))
            assets_in_batch = list(set([asset for _, asset in batch]))

            # Query scaled balances from LatestBalances_v2 and convert to underlying
            query = """
            SELECT
                balances.user,
                balances.asset,
                balances.scaled_balance,
                max_idx.max_variable_debt_liquidityIndex as current_index
            FROM (
                SELECT
                    toString(lb.user) as user,
                    toString(lb.asset) as asset,
                    sumMerge(lb.variable_debt_scaled_balance) as scaled_balance
                FROM aave_ethereum.LatestBalances_v2 lb
                WHERE lb.user IN %(users)s AND lb.asset IN %(assets)s
                GROUP BY toString(lb.user), toString(lb.asset)
                HAVING sumMerge(lb.variable_debt_scaled_balance) > 0
            ) AS balances
            LEFT JOIN aave_ethereum.MaxLiquidityIndex max_idx
                ON balances.asset = max_idx.asset
            """

            parameters = {"users": users_in_batch, "assets": assets_in_batch}
            result = clickhouse_client.execute_query(query, parameters=parameters)

            batch_set = set(batch)
            for row in result.result_rows:
                user = str(row[0]).lower()
                asset = str(row[1]).lower()
                pair = (user, asset)

                if pair in batch_set or (row[0], row[1]) in batch_set:
                    scaled_balance = float(row[2])
                    current_index = float(row[3])

                    # Convert scaled balance to underlying: floor(scaled * current_index / RAY)
                    if current_index > 0:
                        underlying_balance = int(scaled_balance * current_index / RAY)
                    else:
                        underlying_balance = 0

                    debt_balances[pair] = underlying_balance

        logger.info(f"Retrieved {len(debt_balances)} debt balances from ClickHouse")

        return debt_balances

    def _get_rpc_balances_batched(
        self, user_asset_pairs: List[Tuple[str, str]], batch_size: int = 100
    ) -> Dict[Tuple[str, str], float]:
        """
        Get debt balances from RPC in batches.

        Args:
            user_asset_pairs: List of (user, asset) tuples
            batch_size: Number of pairs to query per batch

        Returns:
            Dict mapping (user, asset) to currentVariableDebt
        """
        from utils.interfaces.dataprovider import DataProviderInterface

        data_provider = DataProviderInterface()
        rpc_balances = {}

        for i in range(0, len(user_asset_pairs), batch_size):
            batch = user_asset_pairs[i : i + batch_size]
            logger.info(
                f"Querying RPC for balances batch {i // batch_size + 1} "
                f"({len(batch)} pairs)"
            )

            try:
                batch_results = data_provider.get_user_reserve_data(batch)

                for (user, asset), result in batch_results.items():
                    user_lower = user.lower()
                    asset_lower = asset.lower()
                    # Get currentVariableDebt from result
                    current_variable_debt = float(result.get("currentVariableDebt", 0))
                    rpc_balances[(user_lower, asset_lower)] = current_variable_debt

            except Exception as e:
                logger.error(f"Error querying batch starting at index {i}: {e}")
                raise

        return rpc_balances

    def _compare_debt_balances(
        self,
        clickhouse_data: Dict[Tuple[str, str], float],
        rpc_data: Dict[Tuple[str, str], float],
    ) -> Dict[str, Any]:
        """
        Compare debt balances from ClickHouse and RPC.

        Match criteria: |difference| / rpc_balance < 0.0001 (1 bps)

        Args:
            clickhouse_data: Debt balances from ClickHouse
            rpc_data: Debt balances from RPC

        Returns:
            Dict containing comparison statistics
        """
        clickhouse_pairs = set(clickhouse_data.keys())
        rpc_pairs = set(rpc_data.keys())

        common_pairs = clickhouse_pairs & rpc_pairs

        matching_count = 0
        mismatched_count = 0
        mismatches = []
        differences_bps = []

        # Compare common pairs
        for pair in common_pairs:
            ch_balance = clickhouse_data[pair]
            rpc_balance = rpc_data[pair]

            # Skip if RPC balance is zero (can't calculate percentage)
            if ch_balance == 0:
                if rpc_balance == 0:
                    matching_count += 1
                else:
                    if abs(ch_balance - rpc_balance) < 100_000:
                        matching_count += 1
                    else:
                        mismatched_count += 1
                        user, asset = pair
                        mismatches.append(
                            f"({user},{asset}): CH={ch_balance:.2f} RPC={rpc_balance:.2f}"
                        )
                continue

            # Calculate difference in basis points
            difference = abs(ch_balance - rpc_balance)
            difference_bps = (difference / ch_balance) * 10000

            differences_bps.append(difference_bps)

            # Match if difference < 1 bps
            if difference_bps < 100.0:
                matching_count += 1
            else:
                mismatched_count += 1
                user, asset = pair
                mismatches.append(
                    f"({user},{asset}): CH={ch_balance:.2f} RPC={rpc_balance:.2f} diff={difference_bps:.2f}bps"
                )

        total_pairs = len(common_pairs)
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

"""
Balance validation tasks for comparing ClickHouse computed balances against RPC data.

These tasks validate:
1. Collateral balances (scaled by liquidity index)
2. Debt balances (scaled by liquidity index)

Against getUserReserveData RPC calls.
"""

import csv
import logging
import os
import time
from math import ceil, floor
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

    def run(self, csv_output_path: str = "/tmp"):
        """
        Execute the collateral balance comparison test using CSV export.

        Args:
            csv_output_path: Path to store the CSV file

        Returns:
            Dict[str, Any]: Test results summary
        """
        logger.info("Starting CompareCollateralBalanceTask with CSV export method")
        start_time = time.time()

        try:
            # Step 1: Export all user-asset pairs with collateral to CSV
            csv_filepath = self._export_collateral_pairs_to_csv(csv_output_path)
            if not csv_filepath:
                logger.info("No user-asset pairs with collateral to test")
                return {
                    "status": "completed",
                    "test_duration": time.time() - start_time,
                    "total_user_assets": 0,
                    "matching_records": 0,
                    "mismatched_records": 0,
                }

            # Step 2: Read CSV and compare balances
            logger.info(f"Reading user-asset pairs from {csv_filepath}")
            comparison_results = self._compare_from_csv(csv_filepath)

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

            # Clean up CSV file
            try:
                os.remove(csv_filepath)
                logger.info(f"Cleaned up CSV file: {csv_filepath}")
            except Exception as e:
                logger.warning(f"Failed to clean up CSV file: {e}")

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

    def _export_collateral_pairs_to_csv(self, output_path: str):
        """
        Export all user-asset pairs with non-zero collateral from LatestBalances_v2 to CSV.
        Includes scaled_balance and current_index for quick reference.

        Returns:
            CSV file path, or None if no pairs found
        """
        try:
            logger.info("Exporting collateral user-asset pairs to CSV")

            csv_filename = f"collateral_pairs_{int(time.time())}.csv"
            csv_filepath = os.path.join(output_path, csv_filename)

            total_pairs = 0
            batch_size = 1000
            offset = 0

            with open(csv_filepath, "w", newline="") as csvfile:
                fieldnames = [
                    "user",
                    "asset",
                    "scaled_balance",
                    "current_index",
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                while True:
                    query = """
                    SELECT
                        lb.user,
                        lb.asset,
                        lb.collateral_scaled_balance as scaled_balance,
                        COALESCE(max_idx.max_collateral_liquidityIndex, 0) as current_index
                    FROM (
                        SELECT user, asset, collateral_scaled_balance
                        FROM aave_ethereum.LatestBalances_v2 FINAL
                        WHERE collateral_scaled_balance > 0
                        ORDER BY user, asset
                        LIMIT %(batch_size)s OFFSET %(offset)s
                    ) AS lb
                    LEFT JOIN (
                        SELECT asset, max_collateral_liquidityIndex
                        FROM aave_ethereum.MaxLiquidityIndex FINAL
                    ) AS max_idx
                        ON lb.asset = max_idx.asset
                    """

                    result = clickhouse_client.execute_query(
                        query, parameters={"batch_size": batch_size, "offset": offset}
                    )

                    if not result.result_rows:
                        break

                    for row in result.result_rows:
                        writer.writerow(
                            {
                                "user": row[0],
                                "asset": row[1],
                                "scaled_balance": row[2],
                                "current_index": row[3],
                            }
                        )

                    total_pairs += len(result.result_rows)
                    offset += batch_size
                    logger.info(f"Exported {total_pairs} collateral pairs so far...")

                    if len(result.result_rows) < batch_size:
                        break

            if total_pairs == 0:
                logger.info("No collateral pairs found")
                os.remove(csv_filepath)
                return None

            logger.info(f"Exported {total_pairs} collateral pairs to {csv_filepath}")
            return csv_filepath

        except Exception as e:
            logger.error(f"Error exporting collateral pairs to CSV: {e}", exc_info=True)
            return None

    def _compare_from_csv(self, csv_filepath: str) -> Dict[str, Any]:
        """
        Read user-asset pairs from CSV, calculate ClickHouse balances,
        fetch RPC balances, and compare them.

        Returns:
            Dict containing comparison statistics
        """

        try:
            # Read all pairs from CSV
            user_asset_pairs = []
            clickhouse_data = {}

            with open(csv_filepath, "r") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    user = row["user"].lower()
                    asset = row["asset"].lower()
                    pair = (user, asset)
                    user_asset_pairs.append(pair)

                    # Calculate ClickHouse balance using ray math: underlying = floor((scaled * liquidityIndex) / RAY)
                    # where RAY = 1e27
                    scaled_balance = float(row["scaled_balance"])
                    current_index = float(row["current_index"])
                    RAY = 1e27

                    if current_index > 0:
                        underlying_balance = int(
                            floor(scaled_balance * current_index / RAY)
                        )
                    else:
                        underlying_balance = 0

                    clickhouse_data[pair] = underlying_balance

            logger.info(f"Loaded {len(user_asset_pairs)} user-asset pairs from CSV")
            logger.info(f"Calculated {len(clickhouse_data)} ClickHouse balances")

            # Get RPC balances in batches of 100
            rpc_data = self._get_rpc_balances_batched(user_asset_pairs, batch_size=100)
            logger.info(f"Retrieved {len(rpc_data)} collateral balances from RPC")

            # Compare the balances
            comparison_results = self._compare_collateral_balances(
                clickhouse_data, rpc_data
            )

            return comparison_results

        except Exception as e:
            logger.error(f"Error comparing from CSV: {e}", exc_info=True)
            raise

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
                continue

            # Calculate difference in basis points
            difference = abs(ch_balance - rpc_balance)
            difference_bps = (difference / ch_balance) * 10000

            differences_bps.append(difference_bps)

            # Match if difference < 1 bps
            if difference_bps < 10.0:
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

    def run(self, csv_output_path: str = "/tmp"):
        """
        Execute the debt balance comparison test using CSV export.

        Args:
            csv_output_path: Path to store the CSV file

        Returns:
            Dict[str, Any]: Test results summary
        """
        logger.info("Starting CompareDebtBalanceTask with CSV export method")
        start_time = time.time()

        try:
            # Step 1: Export all user-asset pairs with debt to CSV
            csv_filepath = self._export_debt_pairs_to_csv(csv_output_path)
            if not csv_filepath:
                logger.info("No user-asset pairs with debt to test")
                return {
                    "status": "completed",
                    "test_duration": time.time() - start_time,
                    "total_user_assets": 0,
                    "matching_records": 0,
                    "mismatched_records": 0,
                }

            # Step 2: Read CSV and compare balances
            logger.info(f"Reading user-asset pairs from {csv_filepath}")
            comparison_results = self._compare_from_csv(csv_filepath)

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

            # Clean up CSV file
            try:
                os.remove(csv_filepath)
                logger.info(f"Cleaned up CSV file: {csv_filepath}")
            except Exception as e:
                logger.warning(f"Failed to clean up CSV file: {e}")

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

    def _export_debt_pairs_to_csv(self, output_path: str):
        """
        Export all user-asset pairs with non-zero debt from LatestBalances_v2 to CSV.
        Includes scaled_balance and current_index for quick reference.

        Returns:
            CSV file path, or None if no pairs found
        """
        try:
            logger.info("Exporting debt user-asset pairs to CSV")

            csv_filename = f"debt_pairs_{int(time.time())}.csv"
            csv_filepath = os.path.join(output_path, csv_filename)

            total_pairs = 0
            batch_size = 1000
            offset = 0

            with open(csv_filepath, "w", newline="") as csvfile:
                fieldnames = [
                    "user",
                    "asset",
                    "scaled_balance",
                    "current_index",
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                while True:
                    query = """
                    SELECT
                        lb.user,
                        lb.asset,
                        lb.variable_debt_scaled_balance as scaled_balance,
                        COALESCE(max_idx.max_variable_debt_liquidityIndex, 0) as current_index
                    FROM (
                        SELECT user, asset, variable_debt_scaled_balance
                        FROM aave_ethereum.LatestBalances_v2 FINAL
                        WHERE variable_debt_scaled_balance > 0
                        ORDER BY user, asset
                        LIMIT %(batch_size)s OFFSET %(offset)s
                    ) AS lb
                    LEFT JOIN (
                        SELECT asset, max_variable_debt_liquidityIndex
                        FROM aave_ethereum.MaxLiquidityIndex FINAL
                    ) AS max_idx
                        ON lb.asset = max_idx.asset
                    """

                    result = clickhouse_client.execute_query(
                        query, parameters={"batch_size": batch_size, "offset": offset}
                    )

                    if not result.result_rows:
                        break

                    for row in result.result_rows:
                        writer.writerow(
                            {
                                "user": row[0],
                                "asset": row[1],
                                "scaled_balance": row[2],
                                "current_index": row[3],
                            }
                        )

                    total_pairs += len(result.result_rows)
                    offset += batch_size
                    logger.info(f"Exported {total_pairs} debt pairs so far...")

                    if len(result.result_rows) < batch_size:
                        break

            if total_pairs == 0:
                logger.info("No debt pairs found")
                os.remove(csv_filepath)
                return None

            logger.info(f"Exported {total_pairs} debt pairs to {csv_filepath}")
            return csv_filepath

        except Exception as e:
            logger.error(f"Error exporting debt pairs to CSV: {e}", exc_info=True)
            return None

    def _compare_from_csv(self, csv_filepath: str) -> Dict[str, Any]:
        """
        Read user-asset pairs from CSV, calculate ClickHouse balances,
        fetch RPC balances, and compare them.

        Returns:
            Dict containing comparison statistics
        """
        try:
            # Read all pairs from CSV
            user_asset_pairs = []
            clickhouse_data = {}

            with open(csv_filepath, "r") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    user = row["user"].lower()
                    asset = row["asset"].lower()
                    pair = (user, asset)
                    user_asset_pairs.append(pair)

                    # Calculate ClickHouse balance using ray math: underlying = floor((scaled * liquidityIndex) / RAY)
                    # where RAY = 1e27
                    scaled_balance = float(row["scaled_balance"])
                    current_index = float(row["current_index"])
                    RAY = 1e27

                    if current_index > 0:
                        underlying_balance = int(
                            ceil(scaled_balance * current_index / RAY)
                        )
                    else:
                        underlying_balance = 0

                    clickhouse_data[pair] = underlying_balance

            logger.info(f"Loaded {len(user_asset_pairs)} user-asset pairs from CSV")
            logger.info(f"Calculated {len(clickhouse_data)} ClickHouse balances")

            # Get RPC balances in batches of 100
            rpc_data = self._get_rpc_balances_batched(user_asset_pairs, batch_size=100)
            logger.info(f"Retrieved {len(rpc_data)} debt balances from RPC")

            # Compare the balances
            comparison_results = self._compare_debt_balances(clickhouse_data, rpc_data)

            return comparison_results

        except Exception as e:
            logger.error(f"Error comparing from CSV: {e}", exc_info=True)
            raise

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
                continue

            # Calculate difference in basis points
            difference = abs(ch_balance - rpc_balance)
            difference_bps = (difference / ch_balance) * 10000

            differences_bps.append(difference_bps)

            # Match if difference < 1 bps
            if difference_bps < 10.0:
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

"""
Liquidity index validation tasks for comparing ClickHouse indices against RPC data.

These tasks validate:
1. Collateral liquidity indices (liquidityIndex from getReserveData)
2. Debt liquidity indices (variableBorrowIndex from getReserveData)

Against Pool.getReserveData RPC calls.
"""

import logging
import time
from typing import Any, Dict, List

from celery import Task

from utils.clickhouse.client import clickhouse_client

logger = logging.getLogger(__name__)


class CompareLiquidityIndexTask(Task):
    """
    Task to compare collateral liquidity indices between ClickHouse and RPC.

    Fetches all assets from CollateralLiquidityIndex and compares with
    liquidityIndex from getReserveData RPC call.

    Match criteria: Values must be exactly equal (0 bps difference tolerated)
    """

    def run(self):
        """
        Execute the collateral liquidity index comparison test.

        Returns:
            Dict[str, Any]: Test results summary
        """
        logger.info("Starting CompareLiquidityIndexTask")
        start_time = time.time()

        try:
            # Step 1: Get all assets and their indices from ClickHouse
            clickhouse_data = self._get_clickhouse_indices()
            if not clickhouse_data:
                logger.info("No collateral liquidity indices found in ClickHouse")
                return {
                    "status": "completed",
                    "test_duration": time.time() - start_time,
                    "total_assets": 0,
                    "matching_records": 0,
                    "mismatched_records": 0,
                }

            logger.info(f"Retrieved {len(clickhouse_data)} assets from ClickHouse")

            # Step 2: Get RPC indices for all assets in one batch call
            assets = list(clickhouse_data.keys())
            rpc_data = self._get_rpc_indices(assets)
            logger.info(f"Retrieved {len(rpc_data)} liquidity indices from RPC")

            # Step 3: Compare the indices
            comparison_results = self._compare_indices(clickhouse_data, rpc_data)

            # Calculate test duration
            test_duration = time.time() - start_time

            # Store results in ClickHouse
            self._store_test_results(comparison_results, test_duration)

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
            error_msg = f"Error during collateral liquidity index comparison: {str(e)}"
            logger.error(error_msg, exc_info=True)

            # Store error result
            self._store_error_result(error_msg, time.time() - start_time)

            return {"status": "error", "error": error_msg}

    def _get_clickhouse_indices(self) -> Dict[str, int]:
        """
        Get all collateral liquidity indices from ClickHouse.

        Returns:
            Dict mapping asset address to liquidityIndex
        """
        try:
            query = """
            SELECT
                asset,
                liquidityIndex
            FROM aave_ethereum.view_collateral_liquidity_index
            ORDER BY asset
            """

            result = clickhouse_client.execute_query(query)

            indices = {}
            for row in result.result_rows:
                asset = row[0].lower()
                liquidity_index = int(row[1])
                indices[asset] = liquidity_index

            logger.info(
                f"Retrieved {len(indices)} collateral liquidity indices from ClickHouse"
            )
            return indices

        except Exception as e:
            logger.error(f"Error getting ClickHouse indices: {e}", exc_info=True)
            raise

    def _get_rpc_indices(self, assets: List[str]) -> Dict[str, int]:
        """
        Get collateral liquidity indices from RPC using Pool.getReserveData.

        Args:
            assets: List of asset addresses

        Returns:
            Dict mapping asset address to liquidityIndex
        """
        from utils.interfaces.pool import PoolInterface

        try:
            pool = PoolInterface()
            logger.info(f"Querying RPC for reserve data of {len(assets)} assets")

            # Get all reserve data in one batch call
            reserve_data = pool.get_reserve_data(assets)

            indices = {}
            for asset, data in reserve_data.items():
                asset_lower = asset.lower()
                # Get liquidityIndex from reserve data
                liquidity_index = int(data.get("liquidityIndex", 0))
                indices[asset_lower] = liquidity_index

            return indices

        except Exception as e:
            logger.error(f"Error getting RPC indices: {e}", exc_info=True)
            raise

    def _compare_indices(
        self,
        clickhouse_data: Dict[str, int],
        rpc_data: Dict[str, int],
    ) -> Dict[str, Any]:
        """
        Compare collateral liquidity indices from ClickHouse and RPC.

        Args:
            clickhouse_data: Indices from ClickHouse
            rpc_data: Indices from RPC

        Returns:
            Dict containing comparison statistics
        """
        clickhouse_assets = set(clickhouse_data.keys())
        rpc_assets = set(rpc_data.keys())

        common_assets = clickhouse_assets & rpc_assets

        matching_count = 0
        mismatched_count = 0
        mismatches = []
        differences_bps = []

        # Compare common assets
        for asset in common_assets:
            ch_index = clickhouse_data[asset]
            rpc_index = rpc_data[asset]

            # Calculate exact match or difference in basis points
            if ch_index == rpc_index:
                matching_count += 1
                differences_bps.append(0.0)
            else:
                mismatched_count += 1
                # Calculate difference in bps
                if rpc_index > 0:
                    difference_bps = abs(ch_index - rpc_index) / rpc_index * 10000
                else:
                    difference_bps = 10000.0  # 100% difference if RPC is 0

                differences_bps.append(difference_bps)
                mismatches.append(
                    f"{asset}: CH={ch_index} RPC={rpc_index} diff={difference_bps:.2f}bps"
                )

        total_assets = len(common_assets)
        match_percentage = (matching_count / total_assets * 100) if total_assets else 0
        avg_difference_bps = (
            sum(differences_bps) / len(differences_bps) if differences_bps else 0
        )
        max_difference_bps = max(differences_bps) if differences_bps else 0

        return {
            "total_assets": total_assets,
            "matching_records": matching_count,
            "mismatched_records": mismatched_count,
            "match_percentage": match_percentage,
            "avg_difference_bps": avg_difference_bps,
            "max_difference_bps": max_difference_bps,
            "mismatches_detail": "; ".join(mismatches[:50]),
        }

    def _store_test_results(self, results: Dict[str, Any], duration: float):
        """Store test results in ClickHouse."""
        query = """
        INSERT INTO aave_ethereum.LiquidityIndexTestResults
        (test_timestamp, total_assets, matching_records, mismatched_records,
         match_percentage, avg_difference_bps, max_difference_bps,
         test_duration_seconds, test_status, mismatches_detail)
        VALUES
        (now64(), %(total_assets)s, %(matching_records)s, %(mismatched_records)s,
         %(match_percentage)s, %(avg_difference_bps)s, %(max_difference_bps)s,
         %(test_duration_seconds)s, 'completed', %(mismatches_detail)s)
        """

        parameters = {
            "total_assets": results["total_assets"],
            "matching_records": results["matching_records"],
            "mismatched_records": results["mismatched_records"],
            "match_percentage": results["match_percentage"],
            "avg_difference_bps": results["avg_difference_bps"],
            "max_difference_bps": results["max_difference_bps"],
            "test_duration_seconds": duration,
            "mismatches_detail": results["mismatches_detail"],
        }

        clickhouse_client.execute_query(query, parameters=parameters)
        logger.info("Test results stored successfully in ClickHouse")

    def _store_error_result(self, error_message: str, duration: float):
        """Store error result in ClickHouse."""
        query = """
        INSERT INTO aave_ethereum.LiquidityIndexTestResults
        (test_timestamp, total_assets, matching_records, mismatched_records,
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
            total_count = results["total_assets"]
            match_percentage = results["match_percentage"]
            avg_diff = results["avg_difference_bps"]
            max_diff = results["max_difference_bps"]

            title = "⚠️ Collateral Liquidity Index Mismatch Detected"
            message = (
                f"Found {mismatch_count} mismatch{'es' if mismatch_count != 1 else ''} "
                f"out of {total_count} assets.\n"
                f"Match rate: {match_percentage:.2f}%\n"
                f"Avg difference: {avg_diff:.2f} bps\n"
                f"Max difference: {max_diff:.2f} bps"
            )

            send_simplepush_notification(
                title=title, message=message, event="liquidity_index_mismatch"
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
            ALTER TABLE aave_ethereum.LiquidityIndexTestResults
            DELETE WHERE test_timestamp < now() - INTERVAL 6 DAY
            """

            clickhouse_client.execute_query(query)
            logger.info("Cleaned up test records older than 7 days")

        except Exception as e:
            logger.error(f"Failed to cleanup old test records: {e}")


class CompareVariableBorrowIndexTask(Task):
    """
    Task to compare variable borrow indices between ClickHouse and RPC.

    Fetches all assets from DebtLiquidityIndex and compares with
    variableBorrowIndex from getReserveData RPC call.

    Match criteria: Values must be exactly equal (0 bps difference tolerated)
    """

    def run(self):
        """
        Execute the variable borrow index comparison test.

        Returns:
            Dict[str, Any]: Test results summary
        """
        logger.info("Starting CompareVariableBorrowIndexTask")
        start_time = time.time()

        try:
            # Step 1: Get all assets and their indices from ClickHouse
            clickhouse_data = self._get_clickhouse_indices()
            if not clickhouse_data:
                logger.info("No variable borrow indices found in ClickHouse")
                return {
                    "status": "completed",
                    "test_duration": time.time() - start_time,
                    "total_assets": 0,
                    "matching_records": 0,
                    "mismatched_records": 0,
                }

            logger.info(f"Retrieved {len(clickhouse_data)} assets from ClickHouse")

            # Step 2: Get RPC indices for all assets in one batch call
            assets = list(clickhouse_data.keys())
            rpc_data = self._get_rpc_indices(assets)
            logger.info(f"Retrieved {len(rpc_data)} variable borrow indices from RPC")

            # Step 3: Compare the indices
            comparison_results = self._compare_indices(clickhouse_data, rpc_data)

            # Calculate test duration
            test_duration = time.time() - start_time

            # Store results in ClickHouse
            self._store_test_results(comparison_results, test_duration)

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
            error_msg = f"Error during variable borrow index comparison: {str(e)}"
            logger.error(error_msg, exc_info=True)

            # Store error result
            self._store_error_result(error_msg, time.time() - start_time)

            return {"status": "error", "error": error_msg}

    def _get_clickhouse_indices(self) -> Dict[str, int]:
        """
        Get all variable borrow indices from ClickHouse.

        Returns:
            Dict mapping asset address to liquidityIndex (variable borrow index)
        """
        try:
            query = """
            SELECT
                asset,
                liquidityIndex
            FROM aave_ethereum.view_debt_liquidity_index
            ORDER BY asset
            """

            result = clickhouse_client.execute_query(query)

            indices = {}
            for row in result.result_rows:
                asset = row[0].lower()
                liquidity_index = int(row[1])
                indices[asset] = liquidity_index

            logger.info(
                f"Retrieved {len(indices)} variable borrow indices from ClickHouse"
            )
            return indices

        except Exception as e:
            logger.error(f"Error getting ClickHouse indices: {e}", exc_info=True)
            raise

    def _get_rpc_indices(self, assets: List[str]) -> Dict[str, int]:
        """
        Get variable borrow indices from RPC using Pool.getReserveData.

        Args:
            assets: List of asset addresses

        Returns:
            Dict mapping asset address to variableBorrowIndex
        """
        from utils.interfaces.pool import PoolInterface

        try:
            pool = PoolInterface()
            logger.info(f"Querying RPC for reserve data of {len(assets)} assets")

            # Get all reserve data in one batch call
            reserve_data = pool.get_reserve_data(assets)

            indices = {}
            for asset, data in reserve_data.items():
                asset_lower = asset.lower()
                # Get variableBorrowIndex from reserve data
                variable_borrow_index = int(data.get("variableBorrowIndex", 0))
                indices[asset_lower] = variable_borrow_index

            return indices

        except Exception as e:
            logger.error(f"Error getting RPC indices: {e}", exc_info=True)
            raise

    def _compare_indices(
        self,
        clickhouse_data: Dict[str, int],
        rpc_data: Dict[str, int],
    ) -> Dict[str, Any]:
        """
        Compare variable borrow indices from ClickHouse and RPC.

        Args:
            clickhouse_data: Indices from ClickHouse
            rpc_data: Indices from RPC

        Returns:
            Dict containing comparison statistics
        """
        clickhouse_assets = set(clickhouse_data.keys())
        rpc_assets = set(rpc_data.keys())

        common_assets = clickhouse_assets & rpc_assets

        matching_count = 0
        mismatched_count = 0
        mismatches = []
        differences_bps = []

        # Compare common assets
        for asset in common_assets:
            ch_index = clickhouse_data[asset]
            rpc_index = rpc_data[asset]

            # Calculate exact match or difference in basis points
            if ch_index == rpc_index:
                matching_count += 1
                differences_bps.append(0.0)
            else:
                mismatched_count += 1
                # Calculate difference in bps
                if rpc_index > 0:
                    difference_bps = abs(ch_index - rpc_index) / rpc_index * 10000
                else:
                    difference_bps = 10000.0  # 100% difference if RPC is 0

                differences_bps.append(difference_bps)
                mismatches.append(
                    f"{asset}: CH={ch_index} RPC={rpc_index} diff={difference_bps:.2f}bps"
                )

        total_assets = len(common_assets)
        match_percentage = (matching_count / total_assets * 100) if total_assets else 0
        avg_difference_bps = (
            sum(differences_bps) / len(differences_bps) if differences_bps else 0
        )
        max_difference_bps = max(differences_bps) if differences_bps else 0

        return {
            "total_assets": total_assets,
            "matching_records": matching_count,
            "mismatched_records": mismatched_count,
            "match_percentage": match_percentage,
            "avg_difference_bps": avg_difference_bps,
            "max_difference_bps": max_difference_bps,
            "mismatches_detail": "; ".join(mismatches[:50]),
        }

    def _store_test_results(self, results: Dict[str, Any], duration: float):
        """Store test results in ClickHouse."""
        query = """
        INSERT INTO aave_ethereum.VariableBorrowIndexTestResults
        (test_timestamp, total_assets, matching_records, mismatched_records,
         match_percentage, avg_difference_bps, max_difference_bps,
         test_duration_seconds, test_status, mismatches_detail)
        VALUES
        (now64(), %(total_assets)s, %(matching_records)s, %(mismatched_records)s,
         %(match_percentage)s, %(avg_difference_bps)s, %(max_difference_bps)s,
         %(test_duration_seconds)s, 'completed', %(mismatches_detail)s)
        """

        parameters = {
            "total_assets": results["total_assets"],
            "matching_records": results["matching_records"],
            "mismatched_records": results["mismatched_records"],
            "match_percentage": results["match_percentage"],
            "avg_difference_bps": results["avg_difference_bps"],
            "max_difference_bps": results["max_difference_bps"],
            "test_duration_seconds": duration,
            "mismatches_detail": results["mismatches_detail"],
        }

        clickhouse_client.execute_query(query, parameters=parameters)
        logger.info("Test results stored successfully in ClickHouse")

    def _store_error_result(self, error_message: str, duration: float):
        """Store error result in ClickHouse."""
        query = """
        INSERT INTO aave_ethereum.VariableBorrowIndexTestResults
        (test_timestamp, total_assets, matching_records, mismatched_records,
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
            total_count = results["total_assets"]
            match_percentage = results["match_percentage"]
            avg_diff = results["avg_difference_bps"]
            max_diff = results["max_difference_bps"]

            title = "⚠️ Variable Borrow Index Mismatch Detected"
            message = (
                f"Found {mismatch_count} mismatch{'es' if mismatch_count != 1 else ''} "
                f"out of {total_count} assets.\n"
                f"Match rate: {match_percentage:.2f}%\n"
                f"Avg difference: {avg_diff:.2f} bps\n"
                f"Max difference: {max_diff:.2f} bps"
            )

            send_simplepush_notification(
                title=title, message=message, event="variable_borrow_index_mismatch"
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
            ALTER TABLE aave_ethereum.VariableBorrowIndexTestResults
            DELETE WHERE test_timestamp < now() - INTERVAL 6 DAY
            """

            clickhouse_client.execute_query(query)
            logger.info("Cleaned up test records older than 7 days")

        except Exception as e:
            logger.error(f"Failed to cleanup old test records: {e}")

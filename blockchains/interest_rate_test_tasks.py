"""
Interest rate validation tasks for comparing ClickHouse interest rates against RPC data.

These tasks validate:
1. Collateral interest rates (currentLiquidityRate from getReserveData)
2. Debt interest rates (currentVariableBorrowRate from getReserveData)

Against Pool.getReserveData RPC calls.
"""

import logging
import time
from typing import Any, Dict, List

from celery import Task

from utils.clickhouse.client import clickhouse_client

logger = logging.getLogger(__name__)


class CompareCollateralInterestRateTask(Task):
    """
    Task to compare collateral interest rates between ClickHouse and RPC.

    Fetches all assets from CollateralLiquidityIndex and compares with
    currentLiquidityRate from getReserveData RPC call.

    Match criteria: Values must be exactly equal (0 bps difference tolerated)
    """

    def run(self):
        """
        Execute the collateral interest rate comparison test.

        Returns:
            Dict[str, Any]: Test results summary
        """
        logger.info("Starting CompareCollateralInterestRateTask")
        start_time = time.time()

        try:
            # Step 1: Get all assets and their interest rates from ClickHouse
            clickhouse_data = self._get_clickhouse_rates()
            if not clickhouse_data:
                logger.info("No collateral interest rates found in ClickHouse")
                return {
                    "status": "completed",
                    "test_duration": time.time() - start_time,
                    "total_assets": 0,
                    "matching_records": 0,
                    "mismatched_records": 0,
                }

            logger.info(f"Retrieved {len(clickhouse_data)} assets from ClickHouse")

            # Step 2: Get RPC interest rates for all assets in one batch call
            assets = list(clickhouse_data.keys())
            rpc_data = self._get_rpc_rates(assets)
            logger.info(f"Retrieved {len(rpc_data)} interest rates from RPC")

            # Step 3: Compare the interest rates
            comparison_results = self._compare_rates(clickhouse_data, rpc_data)

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
            error_msg = f"Error during collateral interest rate comparison: {str(e)}"
            logger.error(error_msg, exc_info=True)

            # Store error result
            self._store_error_result(error_msg, time.time() - start_time)

            return {"status": "error", "error": error_msg}

    def _get_clickhouse_rates(self) -> Dict[str, int]:
        """
        Get all collateral interest rates from ClickHouse.

        Returns:
            Dict mapping asset address to interest_rate
        """
        try:
            query = """
            SELECT
                asset,
                interest_rate
            FROM aave_ethereum.view_collateral_liquidity_index
            ORDER BY asset
            """

            result = clickhouse_client.execute_query(query)

            rates = {}
            for row in result.result_rows:
                asset = row[0].lower()
                interest_rate = int(row[1])
                rates[asset] = interest_rate

            logger.info(
                f"Retrieved {len(rates)} collateral interest rates from ClickHouse"
            )
            return rates

        except Exception as e:
            logger.error(f"Error getting ClickHouse rates: {e}", exc_info=True)
            raise

    def _get_rpc_rates(self, assets: List[str]) -> Dict[str, int]:
        """
        Get collateral interest rates from RPC using Pool.getReserveData.

        Args:
            assets: List of asset addresses

        Returns:
            Dict mapping asset address to currentLiquidityRate
        """
        from utils.interfaces.pool import PoolInterface

        try:
            pool = PoolInterface()
            logger.info(f"Querying RPC for reserve data of {len(assets)} assets")

            # Get all reserve data in one batch call
            reserve_data = pool.get_reserve_data(assets)

            rates = {}
            for asset, data in reserve_data.items():
                asset_lower = asset.lower()
                # Get currentLiquidityRate from reserve data
                interest_rate = int(data.get("currentLiquidityRate", 0))
                rates[asset_lower] = interest_rate

            return rates

        except Exception as e:
            logger.error(f"Error getting RPC rates: {e}", exc_info=True)
            raise

    def _compare_rates(
        self,
        clickhouse_data: Dict[str, int],
        rpc_data: Dict[str, int],
    ) -> Dict[str, Any]:
        """
        Compare collateral interest rates from ClickHouse and RPC.

        Args:
            clickhouse_data: Rates from ClickHouse
            rpc_data: Rates from RPC

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
            ch_rate = clickhouse_data[asset]
            rpc_rate = rpc_data[asset]

            # Calculate exact match or difference in basis points
            if ch_rate == rpc_rate:
                matching_count += 1
                differences_bps.append(0.0)
            else:
                mismatched_count += 1
                # Calculate difference in bps
                if rpc_rate > 0:
                    difference_bps = abs(ch_rate - rpc_rate) / rpc_rate * 10000
                else:
                    difference_bps = 10000.0  # 100% difference if RPC is 0

                differences_bps.append(difference_bps)
                mismatches.append(
                    f"{asset}: CH={ch_rate} RPC={rpc_rate} diff={difference_bps:.2f}bps"
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
        INSERT INTO aave_ethereum.CollateralInterestRateTestResults
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
        INSERT INTO aave_ethereum.CollateralInterestRateTestResults
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

            title = "⚠️ Collateral Interest Rate Mismatch Detected"
            message = (
                f"Found {mismatch_count} mismatch{'es' if mismatch_count != 1 else ''} "
                f"out of {total_count} assets.\n"
                f"Match rate: {match_percentage:.2f}%\n"
                f"Avg difference: {avg_diff:.2f} bps\n"
                f"Max difference: {max_diff:.2f} bps"
            )

            send_simplepush_notification(
                title=title, message=message, event="collateral_interest_rate_mismatch"
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
            ALTER TABLE aave_ethereum.CollateralInterestRateTestResults
            DELETE WHERE test_timestamp < now() - INTERVAL 6 DAY
            """

            clickhouse_client.execute_query(query)
            logger.info("Cleaned up test records older than 7 days")

        except Exception as e:
            logger.error(f"Failed to cleanup old test records: {e}")


class CompareDebtInterestRateTask(Task):
    """
    Task to compare debt interest rates between ClickHouse and RPC.

    Fetches all assets from DebtLiquidityIndex and compares with
    currentVariableBorrowRate from getReserveData RPC call.

    Match criteria: Values must be exactly equal (0 bps difference tolerated)
    """

    def run(self):
        """
        Execute the debt interest rate comparison test.

        Returns:
            Dict[str, Any]: Test results summary
        """
        logger.info("Starting CompareDebtInterestRateTask")
        start_time = time.time()

        try:
            # Step 1: Get all assets and their interest rates from ClickHouse
            clickhouse_data = self._get_clickhouse_rates()
            if not clickhouse_data:
                logger.info("No debt interest rates found in ClickHouse")
                return {
                    "status": "completed",
                    "test_duration": time.time() - start_time,
                    "total_assets": 0,
                    "matching_records": 0,
                    "mismatched_records": 0,
                }

            logger.info(f"Retrieved {len(clickhouse_data)} assets from ClickHouse")

            # Step 2: Get RPC interest rates for all assets in one batch call
            assets = list(clickhouse_data.keys())
            rpc_data = self._get_rpc_rates(assets)
            logger.info(f"Retrieved {len(rpc_data)} interest rates from RPC")

            # Step 3: Compare the interest rates
            comparison_results = self._compare_rates(clickhouse_data, rpc_data)

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
            error_msg = f"Error during debt interest rate comparison: {str(e)}"
            logger.error(error_msg, exc_info=True)

            # Store error result
            self._store_error_result(error_msg, time.time() - start_time)

            return {"status": "error", "error": error_msg}

    def _get_clickhouse_rates(self) -> Dict[str, int]:
        """
        Get all debt interest rates from ClickHouse.

        Returns:
            Dict mapping asset address to interest_rate
        """
        try:
            query = """
            SELECT
                asset,
                interest_rate
            FROM aave_ethereum.view_debt_liquidity_index
            ORDER BY asset
            """

            result = clickhouse_client.execute_query(query)

            rates = {}
            for row in result.result_rows:
                asset = row[0].lower()
                interest_rate = int(row[1])
                rates[asset] = interest_rate

            logger.info(f"Retrieved {len(rates)} debt interest rates from ClickHouse")
            return rates

        except Exception as e:
            logger.error(f"Error getting ClickHouse rates: {e}", exc_info=True)
            raise

    def _get_rpc_rates(self, assets: List[str]) -> Dict[str, int]:
        """
        Get debt interest rates from RPC using Pool.getReserveData.

        Args:
            assets: List of asset addresses

        Returns:
            Dict mapping asset address to currentVariableBorrowRate
        """
        from utils.interfaces.pool import PoolInterface

        try:
            pool = PoolInterface()
            logger.info(f"Querying RPC for reserve data of {len(assets)} assets")

            # Get all reserve data in one batch call
            reserve_data = pool.get_reserve_data(assets)

            rates = {}
            for asset, data in reserve_data.items():
                asset_lower = asset.lower()
                # Get currentVariableBorrowRate from reserve data
                interest_rate = int(data.get("currentVariableBorrowRate", 0))
                rates[asset_lower] = interest_rate

            return rates

        except Exception as e:
            logger.error(f"Error getting RPC rates: {e}", exc_info=True)
            raise

    def _compare_rates(
        self,
        clickhouse_data: Dict[str, int],
        rpc_data: Dict[str, int],
    ) -> Dict[str, Any]:
        """
        Compare debt interest rates from ClickHouse and RPC.

        Args:
            clickhouse_data: Rates from ClickHouse
            rpc_data: Rates from RPC

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
            ch_rate = clickhouse_data[asset]
            rpc_rate = rpc_data[asset]

            # Calculate exact match or difference in basis points
            if ch_rate == rpc_rate:
                matching_count += 1
                differences_bps.append(0.0)
            else:
                mismatched_count += 1
                # Calculate difference in bps
                if rpc_rate > 0:
                    difference_bps = abs(ch_rate - rpc_rate) / rpc_rate * 10000
                else:
                    difference_bps = 10000.0  # 100% difference if RPC is 0

                differences_bps.append(difference_bps)
                mismatches.append(
                    f"{asset}: CH={ch_rate} RPC={rpc_rate} diff={difference_bps:.2f}bps"
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
        INSERT INTO aave_ethereum.DebtInterestRateTestResults
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
        INSERT INTO aave_ethereum.DebtInterestRateTestResults
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

            title = "⚠️ Debt Interest Rate Mismatch Detected"
            message = (
                f"Found {mismatch_count} mismatch{'es' if mismatch_count != 1 else ''} "
                f"out of {total_count} assets.\n"
                f"Match rate: {match_percentage:.2f}%\n"
                f"Avg difference: {avg_diff:.2f} bps\n"
                f"Max difference: {max_diff:.2f} bps"
            )

            send_simplepush_notification(
                title=title, message=message, event="debt_interest_rate_mismatch"
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
            ALTER TABLE aave_ethereum.DebtInterestRateTestResults
            DELETE WHERE test_timestamp < now() - INTERVAL 6 DAY
            """

            clickhouse_client.execute_query(query)
            logger.info("Cleaned up test records older than 7 days")

        except Exception as e:
            logger.error(f"Failed to cleanup old test records: {e}")

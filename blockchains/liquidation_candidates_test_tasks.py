"""
Liquidation candidates validation tasks for comparing ClickHouse computed values
against on-chain data via getUserReserveData.

These tasks validate:
1. collateral_balance <= currentATokenBalance from getUserReserveData
2. debt_to_cover <= currentVariableDebt from getUserReserveData
"""

import json
import logging
import time
from typing import Any, Dict, List

from celery import Task

from utils.clickhouse.client import clickhouse_client

logger = logging.getLogger(__name__)


class CompareLiquidationCandidatesTask(Task):
    """
    Task to validate liquidation candidates data quality.

    Validates that:
    - collateral_balance <= currentATokenBalance (ensuring we don't overestimate collateral)
    - debt_to_cover <= currentVariableDebt (ensuring we don't try to cover more debt than exists)

    Match criteria: ClickHouse value <= RPC value
    """

    def run(self, batch_size: int = 50):
        """
        Execute the liquidation candidates validation test.

        Args:
            batch_size: Number of candidates to process per batch

        Returns:
            Dict[str, Any]: Test results summary
        """
        logger.info(
            f"Starting CompareLiquidationCandidatesTask with batch size {batch_size}"
        )
        start_time = time.time()

        try:
            # Get all liquidation candidates
            candidates = self._get_liquidation_candidates()

            if not candidates:
                logger.info("No liquidation candidates found")
                self._store_test_results(
                    {
                        "total_candidates": 0,
                        "matching_records": 0,
                        "mismatched_records": 0,
                        "match_percentage": 100.0,
                        "avg_collateral_difference_bps": 0.0,
                        "max_collateral_difference_bps": 0.0,
                        "avg_debt_difference_bps": 0.0,
                        "max_debt_difference_bps": 0.0,
                        "mismatches": [],
                    },
                    time.time() - start_time,
                    0,
                )
                return {"status": "completed", "total_candidates": 0}

            # Validate candidates in batches
            comparison_results = self._validate_in_batches(candidates, batch_size)

            # Calculate test duration
            test_duration = time.time() - start_time

            # Store results in ClickHouse
            self._store_test_results(comparison_results, test_duration, 0)

            # Clean up old test records
            self._cleanup_old_test_records()

            logger.info(f"Validation completed in {test_duration:.2f} seconds")
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
            error_msg = f"Error during liquidation candidates validation: {str(e)}"
            logger.error(error_msg, exc_info=True)

            # Store error result
            self._store_error_result(error_msg, time.time() - start_time)

            return {"status": "error", "error": error_msg}

    def _get_liquidation_candidates(self) -> List[Dict[str, Any]]:
        """Get all liquidation candidates from Memory table."""
        query = """
        SELECT
            user,
            collateral_asset,
            debt_asset,
            collateral_balance,
            debt_balance,
            debt_to_cover,
            profit
        FROM aave_ethereum.LiquidationCandidates_Memory
        ORDER BY profit DESC
        """

        result = clickhouse_client.execute_query(query)

        candidates = []
        for row in result.result_rows:
            candidates.append(
                {
                    "user": row[0],
                    "collateral_asset": row[1],
                    "debt_asset": row[2],
                    "collateral_balance": float(row[3]),
                    "debt_balance": float(row[4]),
                    "debt_to_cover": float(row[5]),
                    "profit": float(row[6]),
                }
            )

        logger.info(f"Found {len(candidates)} liquidation candidates to validate")
        return candidates

    def _validate_in_batches(
        self, candidates: List[Dict[str, Any]], batch_size: int
    ) -> Dict[str, Any]:
        """
        Validate liquidation candidates in batches using getUserReserveData.

        For each candidate, validate:
        1. collateral_balance <= currentATokenBalance
        2. debt_to_cover <= currentVariableDebt
        """
        from utils.interfaces.dataprovider import DataProviderInterface

        data_provider = DataProviderInterface()

        total_candidates = len(candidates)
        matching_records = 0
        mismatched_records = 0
        collateral_differences = []
        debt_differences = []
        mismatches = []

        # Process in batches
        for i in range(0, total_candidates, batch_size):
            batch = candidates[i : i + batch_size]
            logger.info(
                f"Processing batch {i // batch_size + 1} ({len(batch)} candidates)"
            )

            # Prepare (user, asset) tuples for batch RPC call
            user_asset_pairs_collateral = [
                (c["user"], c["collateral_asset"]) for c in batch
            ]
            user_asset_pairs_debt = [(c["user"], c["debt_asset"]) for c in batch]

            # Combine and deduplicate
            all_pairs = list(set(user_asset_pairs_collateral + user_asset_pairs_debt))

            try:
                # Fetch RPC data for all pairs in batch
                logger.info(f"Fetching RPC data for {len(all_pairs)} user-asset pairs")
                batch_results = data_provider.get_user_reserve_data(all_pairs)

                # Convert to dict for easier lookup
                rpc_data = {}
                for (user, asset), result_data in batch_results.items():
                    user_lower = user.lower()
                    asset_lower = asset.lower()

                    # Get currentATokenBalance and currentVariableDebt from result
                    if isinstance(result_data, dict):
                        current_atoken_balance = float(
                            result_data.get("currentATokenBalance", 0)
                        )
                        current_variable_debt = float(
                            result_data.get("currentVariableDebt", 0)
                        )
                    else:
                        # Result is tuple: (currentATokenBalance, currentStableDebt, currentVariableDebt, ...)
                        current_atoken_balance = float(
                            result_data[0] if len(result_data) > 0 else 0
                        )
                        current_variable_debt = float(
                            result_data[2] if len(result_data) > 2 else 0
                        )

                    rpc_data[(user_lower, asset_lower)] = {
                        "atoken_balance": current_atoken_balance,
                        "variable_debt": current_variable_debt,
                    }

                # Validate each candidate in batch
                for candidate in batch:
                    user_lower = candidate["user"].lower()
                    collateral_asset_lower = candidate["collateral_asset"].lower()
                    debt_asset_lower = candidate["debt_asset"].lower()

                    # Get RPC data
                    collateral_rpc = rpc_data.get(
                        (user_lower, collateral_asset_lower), {}
                    )
                    debt_rpc = rpc_data.get((user_lower, debt_asset_lower), {})

                    collateral_on_chain = collateral_rpc.get("atoken_balance", 0)
                    debt_on_chain = debt_rpc.get("variable_debt", 0)

                    # Check validity: ClickHouse values should be <= RPC values
                    collateral_valid = (
                        candidate["collateral_balance"] <= collateral_on_chain
                    )
                    debt_valid = candidate["debt_to_cover"] <= debt_on_chain

                    # Calculate differences for statistics (in basis points)
                    collateral_diff_bps = 0.0
                    if collateral_on_chain > 0:
                        collateral_diff_bps = (
                            (collateral_on_chain - candidate["collateral_balance"])
                            / collateral_on_chain
                            * 10000
                        )
                        collateral_differences.append(collateral_diff_bps)

                    debt_diff_bps = 0.0
                    if debt_on_chain > 0:
                        debt_diff_bps = (
                            (debt_on_chain - candidate["debt_to_cover"])
                            / debt_on_chain
                            * 10000
                        )
                        debt_differences.append(debt_diff_bps)

                    # Record is valid if both collateral and debt are valid
                    is_valid = collateral_valid and debt_valid

                    if is_valid:
                        matching_records += 1
                    else:
                        mismatched_records += 1
                        mismatches.append(
                            {
                                "user": candidate["user"],
                                "collateral_asset": candidate["collateral_asset"],
                                "debt_asset": candidate["debt_asset"],
                                "ch_collateral_balance": candidate[
                                    "collateral_balance"
                                ],
                                "rpc_collateral_balance": collateral_on_chain,
                                "collateral_valid": collateral_valid,
                                "collateral_diff_bps": collateral_diff_bps,
                                "ch_debt_to_cover": candidate["debt_to_cover"],
                                "rpc_debt_balance": debt_on_chain,
                                "debt_valid": debt_valid,
                                "debt_diff_bps": debt_diff_bps,
                            }
                        )

            except Exception as e:
                logger.error(f"Error processing batch at offset {i}: {e}")
                # Continue to next batch
                continue

            # Small delay between batches
            if i + batch_size < total_candidates:
                time.sleep(0.5)

        # Calculate statistics
        match_percentage = (
            (matching_records / total_candidates * 100)
            if total_candidates > 0
            else 100.0
        )
        avg_collateral_diff = (
            sum(collateral_differences) / len(collateral_differences)
            if collateral_differences
            else 0.0
        )
        max_collateral_diff = (
            max(collateral_differences) if collateral_differences else 0.0
        )
        avg_debt_diff = (
            sum(debt_differences) / len(debt_differences) if debt_differences else 0.0
        )
        max_debt_diff = max(debt_differences) if debt_differences else 0.0

        return {
            "total_candidates": total_candidates,
            "matching_records": matching_records,
            "mismatched_records": mismatched_records,
            "match_percentage": match_percentage,
            "avg_collateral_difference_bps": avg_collateral_diff,
            "max_collateral_difference_bps": max_collateral_diff,
            "avg_debt_difference_bps": avg_debt_diff,
            "max_debt_difference_bps": max_debt_diff,
            "mismatches": mismatches[:100],  # Limit to first 100 mismatches
        }

    def _store_test_results(
        self, results: Dict[str, Any], test_duration: float, attempt: int
    ):
        """Store test results in ClickHouse."""
        try:
            # Prepare mismatches detail
            mismatches_json = json.dumps(
                results.get("mismatches", [])[:20]
            )  # Limit to 20
            mismatches_escaped = mismatches_json.replace("'", "''")

            query = f"""
            INSERT INTO aave_ethereum.LiquidationCandidatesTestResults
            (
                test_timestamp,
                total_candidates,
                matching_records,
                mismatched_records,
                match_percentage,
                avg_collateral_difference_bps,
                max_collateral_difference_bps,
                avg_debt_difference_bps,
                max_debt_difference_bps,
                test_duration_seconds,
                test_status,
                error_message,
                mismatches_detail
            )
            VALUES
            (
                now64(3),
                {results["total_candidates"]},
                {results["matching_records"]},
                {results["mismatched_records"]},
                {results["match_percentage"]},
                {results["avg_collateral_difference_bps"]},
                {results["max_collateral_difference_bps"]},
                {results["avg_debt_difference_bps"]},
                {results["max_debt_difference_bps"]},
                {test_duration},
                'success',
                '',
                '{mismatches_escaped}'
            )
            """

            clickhouse_client.execute_query(query)
            logger.info("Test results stored successfully")

        except Exception as e:
            logger.error(f"Failed to store test results: {e}", exc_info=True)
            if attempt < 2:
                logger.info(f"Retrying storage (attempt {attempt + 1})")
                time.sleep(1)
                self._store_test_results(results, test_duration, attempt + 1)

    def _store_error_result(self, error_msg: str, test_duration: float):
        """Store error result in ClickHouse."""
        try:
            error_escaped = error_msg.replace("'", "''")
            query = f"""
            INSERT INTO aave_ethereum.LiquidationCandidatesTestResults
            (
                test_timestamp,
                total_candidates,
                matching_records,
                mismatched_records,
                match_percentage,
                avg_collateral_difference_bps,
                max_collateral_difference_bps,
                avg_debt_difference_bps,
                max_debt_difference_bps,
                test_duration_seconds,
                test_status,
                error_message,
                mismatches_detail
            )
            VALUES
            (
                now64(3),
                0,
                0,
                0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                {test_duration},
                'error',
                '{error_escaped}',
                ''
            )
            """

            clickhouse_client.execute_query(query)
        except Exception as e:
            logger.error(f"Failed to store error result: {e}")

    def _send_mismatch_notification(self, results: Dict[str, Any]):
        """Send notification when mismatches found."""
        try:
            from utils.simplepush import send_simplepush_notification

            mismatch_count = results["mismatched_records"]
            total_count = results["total_candidates"]
            match_percentage = results["match_percentage"]
            avg_coll_diff = results["avg_collateral_difference_bps"]
            max_coll_diff = results["max_collateral_difference_bps"]
            avg_debt_diff = results["avg_debt_difference_bps"]
            max_debt_diff = results["max_debt_difference_bps"]

            title = "⚠️ Liquidation Candidates Mismatch Detected"
            message = (
                f"Found {mismatch_count} invalid candidate{'s' if mismatch_count != 1 else ''} "
                f"out of {total_count} total.\n"
                f"Match rate: {match_percentage:.2f}%\n"
                f"Avg collateral diff: {avg_coll_diff:.2f} bps\n"
                f"Max collateral diff: {max_coll_diff:.2f} bps\n"
                f"Avg debt diff: {avg_debt_diff:.2f} bps\n"
                f"Max debt diff: {max_debt_diff:.2f} bps"
            )

            send_simplepush_notification(
                title=title, message=message, event="liquidation_candidates_mismatch"
            )

            logger.info(
                f"Sent mismatch notification: {mismatch_count} invalid candidates found"
            )

        except Exception as e:
            logger.error(f"Failed to send mismatch notification: {e}")

    def _cleanup_old_test_records(self):
        """Delete test records older than 7 days."""
        try:
            query = """
            ALTER TABLE aave_ethereum.LiquidationCandidatesTestResults
            DELETE WHERE test_timestamp < now() - INTERVAL 6 DAY
            """

            clickhouse_client.execute_query(query)
            logger.info("Cleaned up test records older than 7 days")

        except Exception as e:
            logger.error(f"Failed to cleanup old test records: {e}")

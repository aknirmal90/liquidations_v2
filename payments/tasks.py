import logging
from datetime import datetime
from typing import Any, Dict, List

from celery import Task

from liquidations_v2.celery_app import app
from utils.clickhouse.client import clickhouse_client
from utils.simplepush import send_simplepush_notification

logger = logging.getLogger(__name__)


class EstimateFutureLiquidationCandidatesTask(Task):
    """
    Task to estimate future liquidation candidates after transaction numerator updates.

    This task:
    1. Identifies assets that were updated in the transaction numerator
    2. Calculates health factors using predicted_transaction_price for updated assets
       and historical_event_price for other assets
    3. Finds users who have health_factor > 1 on view 146 (current)
       but health_factor < 1 using predicted prices
    4. Retrieves liquidation candidates from LiquidationCandidates_Memory for these users
    5. Appends results to ClickHouse LiquidationDetections log table
    6. Sends a SimplePush notification with summary
    """

    clickhouse_client = clickhouse_client

    def run(self, parsed_numerator_logs: List[Any]):
        """
        Run the task to identify future liquidation candidates.

        Args:
            parsed_numerator_logs: List of parsed transaction numerator logs
                Each log contains: [asset, asset_source, asset_source_type, timestamp, blockNumber, transaction_hash, type, price]
        """
        try:
            logger.info(
                "[LIQUIDATION_DETECTION] Starting EstimateFutureLiquidationCandidatesTask"
            )

            # Step 1: Extract updated assets and transaction metadata from the logs
            updated_assets, transaction_hashes, block_numbers = (
                self._extract_updated_assets_and_metadata(parsed_numerator_logs)
            )
            if not updated_assets:
                logger.info(
                    "[LIQUIDATION_DETECTION] No assets to process, skipping task"
                )
                return

            max_block_number = max(block_numbers) if block_numbers else 0

            logger.info(
                f"[LIQUIDATION_DETECTION] Processing {len(updated_assets)} updated assets: {', '.join(updated_assets[:5])}"
                + (
                    f" ... and {len(updated_assets) - 5} more"
                    if len(updated_assets) > 5
                    else ""
                )
            )

            # Step 2: Get liquidation candidates in a single optimized query
            # This combines the at-risk users query and liquidation candidates lookup
            liquidation_candidates = self._get_liquidation_candidates_optimized(
                updated_assets, transaction_hashes, max_block_number
            )

            if not liquidation_candidates:
                logger.info("[LIQUIDATION_DETECTION] No liquidation candidates found")
                return

            # Step 3: Append results to ClickHouse LiquidationDetections log table
            self._append_liquidation_detections(liquidation_candidates)

            # Calculate summary statistics
            num_users = len(set([c[0] for c in liquidation_candidates]))
            total_profit = sum([float(c[6]) for c in liquidation_candidates])

            # Step 4: Send SimplePush notification
            self._send_notification(liquidation_candidates, updated_assets)

            logger.warning(
                f"[LIQUIDATION_DETECTED] *** LIQUIDATION OPPORTUNITIES FOUND *** "
                f"Users: {num_users} | Opportunities: {len(liquidation_candidates)} | "
                f"Potential Profit: ${total_profit:,.2f}"
            )

        except Exception as e:
            logger.error(
                f"[LIQUIDATION_DETECTION_ERROR] Error in EstimateFutureLiquidationCandidatesTask: {e}",
                exc_info=True,
            )

    def _extract_updated_assets_and_metadata(self, parsed_numerator_logs: List[Any]):
        """
        Extract unique asset addresses, transaction hashes, and block numbers from parsed logs.

        Args:
            parsed_numerator_logs: List where each log is [asset, asset_source, asset_source_type,
                                   timestamp, blockNumber, transaction_hash, type, price]

        Returns:
            Tuple of (assets list, transaction_hashes list, block_numbers list)
        """
        assets = set()
        transaction_hashes = set()
        block_numbers = set()

        for log in parsed_numerator_logs:
            if len(log) >= 6:
                asset = log[0]  # asset is the first element
                block_number = log[4]  # blockNumber is at index 4
                transaction_hash = log[5]  # transaction_hash is at index 5

                assets.add(asset)
                if transaction_hash:
                    transaction_hashes.add(transaction_hash)
                if block_number:
                    block_numbers.add(block_number)

        return list(assets), list(transaction_hashes), list(block_numbers)

    def _get_at_risk_users_with_predicted_prices(
        self, updated_assets: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Calculate health factors using predicted prices for updated assets
        and find users with current HF > 1 but predicted HF < 1.

        Returns:
            List of dicts with keys: user, current_health_factor, predicted_health_factor
        """
        # Build CASE statement for dynamic price selection
        # For updated assets, use predicted_transaction_price; for others, use historical_event_price
        assets_str = ", ".join([f"'{asset}'" for asset in updated_assets])

        query = f"""
        WITH
        asset_effective_balances AS (
            SELECT
                uaeb.user,
                uaeb.asset,
                uaeb.accrued_collateral_balance,
                uaeb.accrued_debt_balance,
                -- eMode status
                dictGetOrDefault('aave_ethereum.dict_emode_status', 'is_enabled_in_emode', toString(uaeb.user), toInt8(0)) AS is_in_emode,
                -- Asset configuration
                dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'decimals_places', uaeb.asset, toUInt256(1)) AS decimals_places,
                -- Use predicted_transaction_price for updated assets, historical_event_price for others
                -- Cast predicted_transaction_price (UInt256) to Float64 for type compatibility
                if(
                    uaeb.asset IN ({assets_str}),
                    toFloat64(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'predicted_transaction_price', uaeb.asset, toUInt256(0))),
                    toFloat64(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'historical_event_price', uaeb.asset, toFloat64(0)))
                ) AS price,
                dictGetOrDefault('aave_ethereum.dict_collateral_status', 'is_enabled_as_collateral', tuple(uaeb.user, uaeb.asset), toInt8(0)) AS is_collateral_enabled,
                -- Liquidation thresholds
                dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'eModeLiquidationThreshold', uaeb.asset, toUInt256(0)) AS emode_liquidation_threshold,
                dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'collateralLiquidationThreshold', uaeb.asset, toUInt256(0)) AS collateral_liquidation_threshold
            FROM aave_ethereum.view_future_user_asset_effective_balances AS uaeb
        ),
        effective_balances AS (
            SELECT
                user,
                asset,
                is_in_emode,
                accrued_collateral_balance,
                accrued_debt_balance,
                -- Effective Collateral: apply liquidation threshold based on eMode, collateral status, and price
                cast(floor(
                    toFloat64(accrued_collateral_balance)
                    * if(
                        is_in_emode = 1,
                        toFloat64(emode_liquidation_threshold),
                        toFloat64(collateral_liquidation_threshold)
                    )
                    * toFloat64(is_collateral_enabled)
                    * price
                    / (10000 * toFloat64(decimals_places))
                ) as UInt256) AS effective_collateral,
                -- Effective Debt: apply price adjustment
                cast(floor(
                    toFloat64(accrued_debt_balance)
                    * price
                    / (toFloat64(decimals_places))
                ) as UInt256) AS effective_debt,
                cast(floor(
                    toFloat64(accrued_collateral_balance)
                    * if(
                        is_in_emode = 1,
                        toFloat64(emode_liquidation_threshold),
                        toFloat64(collateral_liquidation_threshold)
                    )
                    * toFloat64(is_collateral_enabled)
                    * price
                    / (10000 * toFloat64(decimals_places) * toFloat64(decimals_places))
                ) as UInt256) AS effective_collateral_usd,
                -- Effective Debt: apply price adjustment
                cast(floor(
                    toFloat64(accrued_debt_balance)
                    * price
                    / (toFloat64(decimals_places) * toFloat64(decimals_places))
                ) as UInt256) AS effective_debt_usd
            FROM asset_effective_balances
        ),
        predicted_health_factors AS (
            SELECT
                user,
                is_in_emode,
                sum(effective_collateral) AS total_effective_collateral,
                sum(effective_debt) AS total_effective_debt,
                if(
                    total_effective_debt = 0,
                    999.9,
                    total_effective_collateral / total_effective_debt
                ) AS predicted_health_factor
            FROM effective_balances
            WHERE effective_collateral_usd > 10000
                AND effective_debt_usd > 10000
            GROUP BY user, is_in_emode
        ),
        current_health_factors AS (
            SELECT
                user,
                health_factor AS current_health_factor
            FROM aave_ethereum.view_user_health_factor
            WHERE
                effective_collateral_usd > 10000
                AND effective_debt_usd > 10000
        )
        SELECT
            phf.user,
            chf.current_health_factor,
            phf.predicted_health_factor
        FROM predicted_health_factors AS phf
        INNER JOIN current_health_factors AS chf ON phf.user = chf.user
        WHERE
            chf.current_health_factor > 1.0
            AND phf.predicted_health_factor <= 1.0
        """

        try:
            result = self.clickhouse_client.execute_query(query)
            at_risk_users = []

            if result.result_rows:
                logger.info(
                    f"[LIQUIDATION_DETECTION] Query returned {len(result.result_rows)} users with declining health factors"
                )

                for row in result.result_rows:
                    user = row[0]
                    current_hf = float(row[1])
                    predicted_hf = float(row[2])

                    at_risk_users.append(
                        {
                            "user": user,
                            "current_health_factor": current_hf,
                            "predicted_health_factor": predicted_hf,
                        }
                    )

                    # Log individual user at risk with health factor change
                    logger.info(
                        f"[LIQUIDATION_USER_RISK] User: {user} | "
                        f"Current HF: {current_hf:.4f} → Predicted HF: {predicted_hf:.4f} | "
                        f"Change: {((predicted_hf - current_hf) / current_hf * 100):.2f}%"
                    )

            return at_risk_users

        except Exception as e:
            logger.error(
                f"[LIQUIDATION_DETECTION_ERROR] Error calculating at-risk users: {e}",
                exc_info=True,
            )
            return []

    def _get_liquidation_candidates_from_memory(
        self, at_risk_users: List[Dict[str, Any]], updated_assets: List[str]
    ) -> List[List[Any]]:
        """
        Retrieve liquidation candidates from LiquidationCandidates_Memory table
        for the at-risk users.

        Returns:
            List of rows for insertion into LiquidationDetections log table
        """
        # Extract user addresses
        user_addresses = [user["user"] for user in at_risk_users]
        users_str = ", ".join([f"'{user}'" for user in user_addresses])

        # Create a map of user -> health factors for easy lookup
        health_factor_map = {
            user["user"]: (
                user["current_health_factor"],
                user["predicted_health_factor"],
            )
            for user in at_risk_users
        }

        query = f"""
        SELECT
            user,
            collateral_asset,
            debt_asset,
            debt_to_cover,
            profit,
            effective_collateral,
            effective_debt,
            collateral_balance,
            debt_balance,
            liquidation_bonus,
            collateral_price,
            debt_price,
            collateral_decimals,
            debt_decimals,
            is_priority_debt,
            is_priority_collateral
        FROM aave_ethereum.LiquidationCandidates_Memory
        WHERE user IN ({users_str})
        ORDER BY profit DESC
        """

        try:
            result = self.clickhouse_client.execute_query(query)
            candidates = []

            if result.result_rows:
                logger.info(
                    f"[LIQUIDATION_DETECTION] Retrieved {len(result.result_rows)} liquidation opportunities from memory table"
                )

                for row in result.result_rows:
                    user = row[0]
                    collateral_asset = row[1]
                    debt_asset = row[2]
                    profit = float(row[4])
                    current_hf, predicted_hf = health_factor_map.get(user, (0.0, 0.0))

                    # Format row for insertion into LiquidationDetections log table
                    candidates.append(
                        [
                            user,  # user
                            collateral_asset,  # collateral_asset
                            debt_asset,  # debt_asset
                            current_hf,  # current_health_factor
                            predicted_hf,  # predicted_health_factor
                            row[3],  # debt_to_cover
                            profit,  # profit
                            row[5],  # effective_collateral
                            row[6],  # effective_debt
                            row[7],  # collateral_balance
                            row[8],  # debt_balance
                            row[9],  # liquidation_bonus
                            row[10],  # collateral_price
                            row[11],  # debt_price
                            row[12],  # collateral_decimals
                            row[13],  # debt_decimals
                            row[14],  # is_priority_debt
                            row[15],  # is_priority_collateral
                            updated_assets,  # updated_assets
                            int(
                                datetime.now().timestamp()
                            ),  # detected_at (Unix timestamp)
                        ]
                    )

                    # Log individual liquidation opportunity
                    logger.warning(
                        f"[LIQUIDATION_OPPORTUNITY] User: {user[:10]}... | "
                        f"Collateral: {collateral_asset[:10]}... | Debt: {debt_asset[:10]}... | "
                        f"Profit: ${profit:,.2f} | HF: {current_hf:.4f} → {predicted_hf:.4f}"
                    )

            return candidates

        except Exception as e:
            logger.error(
                f"[LIQUIDATION_DETECTION_ERROR] Error getting liquidation candidates: {e}",
                exc_info=True,
            )
            return []

    def _append_liquidation_detections(self, candidates: List[List[Any]]):
        """Append liquidation detections to ClickHouse Log table."""
        try:
            # Simply insert rows into the log table (no need for atomic swap with Log engine)
            self.clickhouse_client.insert_rows("LiquidationDetections", candidates)

            logger.info(
                f"[LIQUIDATION_DETECTION] Successfully appended {len(candidates)} liquidation detections to log table"
            )

        except Exception as e:
            logger.error(
                f"[LIQUIDATION_DETECTION_ERROR] Error appending liquidation detections: {e}",
                exc_info=True,
            )

    def _send_notification(
        self, candidates: List[List[Any]], updated_assets: List[str]
    ):
        """Send SimplePush notification with summary."""
        try:
            num_candidates = len(candidates)
            num_users = len(set([c[0] for c in candidates]))
            # profit is at index 6
            total_profit = sum([float(c[6]) for c in candidates])

            title = f"Predicted Liquidation Alert: {num_users} Users at Risk"
            message = (
                f"Found {num_candidates} liquidation opportunities for {num_users} users.\n"
                f"Total potential profit: ${total_profit:,.2f}\n"
                f"Updated assets: {len(updated_assets)}\n"
                f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

            send_simplepush_notification(
                title=title, message=message, event="predicted_liquidation_alert"
            )

            logger.info(
                f"[LIQUIDATION_NOTIFICATION_SENT] SimplePush notification sent | "
                f"Users: {num_users} | Opportunities: {num_candidates} | Profit: ${total_profit:,.2f}"
            )

        except Exception as e:
            logger.error(
                f"[LIQUIDATION_DETECTION_ERROR] Error sending notification: {e}",
                exc_info=True,
            )


EstimateFutureLiquidationCandidatesTask = app.register_task(
    EstimateFutureLiquidationCandidatesTask()
)

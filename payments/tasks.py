import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from itertools import combinations
from typing import Any, List, Optional

from celery import Task
from web3 import Web3

from liquidations_v2.celery_app import app
from payments.liquidation_executor import ExecuteLiquidationsTask
from utils.clickhouse.client import clickhouse_client
from utils.interfaces.base import BaseContractInterface
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
            liquidation_candidates = self._get_at_risk_users_with_predicted_prices(
                updated_assets
            )

            if not liquidation_candidates:
                logger.info("[LIQUIDATION_DETECTION] No liquidation candidates found")
                return

            # Step 3: Append results to ClickHouse LiquidationDetections log table
            self._append_liquidation_detections(liquidation_candidates)

            # Step 4: Convert liquidation_candidates to opportunities format for executor
            opportunities = []
            for row in liquidation_candidates:
                opportunities.append(
                    {
                        "user": row[0],
                        "collateral_asset": row[1],
                        "debt_asset": row[2],
                        "current_health_factor": float(row[3]),
                        "predicted_health_factor": float(row[4]),
                        "debt_to_cover": float(row[5]),
                        "profit": float(row[6]),
                        "effective_collateral": float(row[7]),
                        "effective_debt": float(row[8]),
                        "collateral_balance": float(row[9]),
                        "debt_balance": float(row[10]),
                        "liquidation_bonus": int(row[11]),
                        "collateral_price": float(row[12]),
                        "debt_price": float(row[13]),
                        "collateral_decimals": int(row[14]),
                        "debt_decimals": int(row[15]),
                    }
                )

            # Calculate summary statistics
            num_users = len(set([c[0] for c in liquidation_candidates]))
            total_profit = sum([float(c[6]) for c in liquidation_candidates])

            # Get detection timestamp for execution tracking
            detection_timestamp = int(datetime.now().timestamp())

            # Step 5: Send SimplePush notification
            self._send_notification(liquidation_candidates, updated_assets)

            # Step 7: Trigger liquidation execution with opportunities data
            ExecuteLiquidationsTask.delay(
                opportunities=opportunities,
                detection_timestamp=detection_timestamp,
                updated_assets=updated_assets,
            )

            logger.warning(
                f"[LIQUIDATION_DETECTED] *** LIQUIDATION OPPORTUNITIES FOUND *** "
                f"Users: {num_users} | Opportunities: {len(liquidation_candidates)} | "
                f"Potential Profit: ${total_profit:,.2f} | "
                f"Execution triggered for timestamp: {detection_timestamp}"
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
    ) -> List[List[Any]]:
        """
        Calculate health factors using predicted prices for updated assets,
        find users with current HF > 1 but predicted HF < 1,
        and retrieve their liquidation candidates from LiquidationCandidates_Memory.

        Returns:
            List of lists containing liquidation detection data ready for insertion
            into LiquidationDetections table. Each row contains:
            [user, collateral_asset, debt_asset, current_health_factor, predicted_health_factor,
             debt_to_cover, profit, effective_collateral, effective_debt, collateral_balance,
             debt_balance, liquidation_bonus, collateral_price, debt_price, collateral_decimals,
             debt_decimals, updated_assets, detected_at]
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
                (
                    accrued_collateral_balance
                    * toDecimal256(
                        if(
                            is_in_emode = 1,
                            emode_liquidation_threshold,
                            collateral_liquidation_threshold
                        ), 0
                    )
                    * toDecimal256(is_collateral_enabled, 0)
                    * toDecimal256(price, 18)
                    / (toDecimal256(10000, 0) * toDecimal256(decimals_places, 0))
                ) AS effective_collateral,
                -- Effective Debt: apply price adjustment
                (
                    accrued_debt_balance
                    * toDecimal256(price, 18)
                    / toDecimal256(decimals_places, 0)
                ) AS effective_debt,
                (
                    accrued_collateral_balance
                    * toDecimal256(
                        if(
                            is_in_emode = 1,
                            emode_liquidation_threshold,
                            collateral_liquidation_threshold
                        ), 0
                    )
                    * toDecimal256(is_collateral_enabled, 0)
                    * toDecimal256(price, 18)
                    / (toDecimal256(10000, 0) * toDecimal256(decimals_places, 0) * toDecimal256(1e8, 0))
                ) AS effective_collateral_usd,
                -- Effective Debt: apply price adjustment
                (
                    accrued_debt_balance
                    * toDecimal256(price, 18)
                    / (toDecimal256(decimals_places, 0) * toDecimal256(1e8, 0))
                ) AS effective_debt_usd
            FROM asset_effective_balances
        ),
        predicted_health_factors AS (
            SELECT
                user,
                is_in_emode,
                sum(effective_collateral_usd) AS total_effective_collateral_usd,
                sum(effective_debt_usd) AS total_effective_debt_usd,
                sum(effective_collateral) AS total_effective_collateral,
                sum(effective_debt) AS total_effective_debt,
                if(
                    sum(effective_debt) = 0,
                    toDecimal256(999.9, 18),
                    toDecimal256(sum(effective_collateral), 0) / toDecimal256(sum(effective_debt), 0)
                ) AS predicted_health_factor
            FROM effective_balances
            GROUP BY user, is_in_emode
        ),
        current_health_factors AS (
            SELECT
                user,
                effective_collateral_usd,
                effective_debt_usd,
                health_factor AS current_health_factor
            FROM aave_ethereum.view_user_health_factor
        ),
        at_risk_users AS (
            SELECT
                phf.user AS user,
                chf.current_health_factor AS current_health_factor,
                phf.predicted_health_factor AS predicted_health_factor
            FROM predicted_health_factors AS phf
            INNER JOIN current_health_factors AS chf ON phf.user = chf.user
            WHERE
                chf.current_health_factor > 1.0
                AND phf.predicted_health_factor <= 1.0
                AND phf.total_effective_collateral_usd > 10000
                AND phf.total_effective_debt_usd > 10000
                AND chf.effective_collateral_usd > 10000
                AND chf.effective_debt_usd > 10000
        )
        SELECT
            lc.user AS user,
            lc.collateral_asset AS collateral_asset,
            lc.debt_asset AS debt_asset,
            aru.current_health_factor AS current_health_factor,
            aru.predicted_health_factor AS predicted_health_factor,
            lc.debt_to_cover AS debt_to_cover,
            lc.profit AS profit,
            lc.effective_collateral AS effective_collateral,
            lc.effective_debt AS effective_debt,
            lc.collateral_balance AS collateral_balance,
            lc.debt_balance AS debt_balance,
            lc.liquidation_bonus AS liquidation_bonus,
            lc.collateral_price AS collateral_price,
            lc.debt_price AS debt_price,
            lc.collateral_decimals AS collateral_decimals,
            lc.debt_decimals AS debt_decimals
        FROM aave_ethereum.LiquidationCandidates_Memory AS lc
        INNER JOIN at_risk_users AS aru ON lc.user = aru.user
        ORDER BY lc.profit DESC
        """

        try:
            # Execute query and get result as list of dictionaries for key-value pairs
            result = self.clickhouse_client.execute_query(query)

            # fallback to rows with column names if available
            rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
            if rows:
                num_users = len(set([row[0] for row in result.result_rows]))
                logger.info(
                    f"[LIQUIDATION_DETECTION] Found {len(result.result_rows)} liquidation opportunities "
                    f"for {num_users} users with declining health factors"
                )

                for row in rows:
                    user = row["user"]
                    current_hf = float(row["current_health_factor"])
                    predicted_hf = float(row["predicted_health_factor"])
                    profit = float(row["profit"])

                    # Log individual liquidation opportunity
                    logger.info(
                        f"[LIQUIDATION_OPPORTUNITY] User: {user} | "
                        f"Current HF: {current_hf:.4f} → Predicted HF: {predicted_hf:.4f} | "
                        f"Profit: ${profit:,.2f}"
                    )

            return rows

        except Exception as e:
            logger.error(
                f"[LIQUIDATION_DETECTION_ERROR] Error calculating liquidation opportunities: {e}",
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


class UpdateSwapPathsTask(Task):
    """
    Task to update optimal swap paths between priority assets.

    This task:
    1. Loads pool data from payments/pools.json
    2. Finds all 1-hop and 2-hop paths between priority assets
    3. Queries Uniswap V3 Quoter for real-time quotes
    4. Selects the best path for each token pair
    5. Writes results to a temporary ClickHouse table
    6. Atomically swaps temp table with SwapPaths_Memory
    7. Drops the temp table

    Runs every second to keep swap paths up-to-date.
    """

    # Priority assets
    PRIORITY_ASSETS = {
        "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
    }

    # Token decimals
    TOKEN_DECIMALS = {
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": 18,  # WETH
        "0xdac17f958d2ee523a2206206994597c13d831ec7": 6,  # USDT
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 6,  # USDC
        "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": 8,  # WBTC
    }

    QUOTER_V2_ADDRESS = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"
    POOLS_FILE = "payments/pools.json"

    clickhouse_client = clickhouse_client

    def run(self):
        """Run the task to update swap paths."""
        try:
            logger.info("[SWAP_PATHS] Starting UpdateSwapPathsTask")

            # Load pools
            pool_data = self._load_pools()

            # Generate all token pairs (unique pairs only, not reverse)
            token_pairs = list(combinations(self.PRIORITY_ASSETS.values(), 2))

            logger.info(
                f"[SWAP_PATHS] Processing {len(token_pairs)} unique token pairs"
            )

            # Build graph from pools
            graph = self._build_graph(pool_data["pools"])

            # Initialize Quoter
            self.quoter = BaseContractInterface(self.QUOTER_V2_ADDRESS)

            # Compute best paths for each direction
            best_paths = []
            for token_a, token_b in token_pairs:
                # Find best path from A to B
                path_a_to_b = self._find_best_path(token_a, token_b, graph)

                # Find best path from B to A
                path_b_to_a = self._find_best_path(token_b, token_a, graph)

                # Use the same pool path for both directions
                # (Choose the direction with better output, but use same pools)
                if path_a_to_b and path_b_to_a:
                    # Both directions have paths - use the same pools for both
                    # The pool path is the same regardless of direction
                    pool_path = path_a_to_b["path"]

                    best_paths.append(
                        {
                            "token_in": token_a,
                            "token_out": token_b,
                            "path": pool_path,
                        }
                    )
                    best_paths.append(
                        {
                            "token_in": token_b,
                            "token_out": token_a,
                            "path": pool_path,  # Same pool path for reverse direction
                        }
                    )
                elif path_a_to_b:
                    # Only A->B has a path
                    pool_path = path_a_to_b["path"]
                    best_paths.append(path_a_to_b)
                    # Store reverse direction with same path
                    best_paths.append(
                        {
                            "token_in": token_b,
                            "token_out": token_a,
                            "path": pool_path,
                        }
                    )
                elif path_b_to_a:
                    # Only B->A has a path
                    pool_path = path_b_to_a["path"]
                    best_paths.append(path_b_to_a)
                    # Store reverse direction with same path
                    best_paths.append(
                        {
                            "token_in": token_a,
                            "token_out": token_b,
                            "path": pool_path,
                        }
                    )

            logger.info(
                f"[SWAP_PATHS] Found {len(best_paths)} optimal paths (bidirectional)"
            )

            # Write to temporary table and swap
            self._update_swap_paths_table(best_paths)

            logger.info("[SWAP_PATHS] Successfully updated swap paths")

        except Exception as e:
            logger.error(
                f"[SWAP_PATHS_ERROR] Error in UpdateSwapPathsTask: {e}", exc_info=True
            )

    def _load_pools(self):
        """Load pool data from JSON file."""
        pools_path = os.path.join(os.path.dirname(__file__), "..", self.POOLS_FILE)
        if not os.path.exists(pools_path):
            raise FileNotFoundError(f"Pools file not found: {pools_path}")

        with open(pools_path, "r") as f:
            return json.load(f)

    def _build_graph(self, pools):
        """Build graph from pools."""
        graph = defaultdict(list)

        for pool in pools:
            if not pool.get("has_liquidity"):
                continue

            token0 = pool["token0"].lower()
            token1 = pool["token1"].lower()

            # Both directions
            graph[token0].append(
                {
                    "neighbor": token1,
                    "pool_address": pool["pool_address"],
                    "fee": pool["fee"],
                }
            )
            graph[token1].append(
                {
                    "neighbor": token0,
                    "pool_address": pool["pool_address"],
                    "fee": pool["fee"],
                }
            )

        return graph

    def _find_best_path(self, token_in: str, token_out: str, graph):
        """Find best path between two tokens."""
        token_in_lower = token_in.lower()

        # Get amount in (1 unit of input token)
        decimals = self.TOKEN_DECIMALS.get(token_in_lower)
        if not decimals:
            return None
        amount_in = 10**decimals

        # Find all paths
        one_hop_paths = self._find_one_hop_paths(token_in, token_out, graph)
        two_hop_paths = self._find_two_hop_paths(token_in, token_out, graph)

        # Quote paths
        all_paths = []

        for path_info in one_hop_paths:
            quote = self._quote_exact_input_single(
                path_info["token_in"],
                path_info["token_out"],
                amount_in,
                path_info["fee"],
                path_info["pool_address"],
            )
            if quote:
                path_info["output_amount"] = quote["amount_out"]
                path_info["hops"] = 1
                all_paths.append(path_info)

        for path_info in two_hop_paths:
            quote = self._quote_exact_input_multi_hop(
                [
                    path_info["token_in"],
                    path_info["intermediate"],
                    path_info["token_out"],
                ],
                [path_info["fee1"], path_info["fee2"]],
                amount_in,
            )
            if quote:
                path_info["output_amount"] = quote["amount_out"]
                path_info["hops"] = 2
                all_paths.append(path_info)

        if not all_paths:
            return None

        # Sort by output amount and get best
        all_paths.sort(key=lambda x: x["output_amount"], reverse=True)
        best = all_paths[0]

        # Build path string (semicolon-separated pool addresses)
        if best["hops"] == 1:
            path_str = best["pool_address"]
        else:
            path_str = f"{best['pool1_address']};{best['pool2_address']}"

        return {
            "token_in": token_in,
            "token_out": token_out,
            "path": path_str,
        }

    def _find_one_hop_paths(self, from_addr, to_addr, graph):
        """Find all 1-hop paths."""
        from_lower = from_addr.lower()
        to_lower = to_addr.lower()

        paths = []
        if from_lower in graph:
            for edge in graph[from_lower]:
                if edge["neighbor"] == to_lower:
                    paths.append(
                        {
                            "token_in": from_addr,
                            "token_out": to_addr,
                            "pool_address": edge["pool_address"],
                            "fee": edge["fee"],
                        }
                    )

        return paths

    def _find_two_hop_paths(self, from_addr, to_addr, graph):
        """Find all 2-hop paths."""
        from_lower = from_addr.lower()
        to_lower = to_addr.lower()

        paths = []
        if from_lower not in graph:
            return paths

        for first_hop in graph[from_lower]:
            intermediate = first_hop["neighbor"]

            if intermediate == to_lower:
                continue

            if intermediate in graph:
                for second_hop in graph[intermediate]:
                    if second_hop["neighbor"] == to_lower:
                        paths.append(
                            {
                                "token_in": from_addr,
                                "intermediate": Web3.to_checksum_address(intermediate),
                                "token_out": to_addr,
                                "pool1_address": first_hop["pool_address"],
                                "pool2_address": second_hop["pool_address"],
                                "fee1": first_hop["fee"],
                                "fee2": second_hop["fee"],
                            }
                        )

        return paths

    def _quote_exact_input_single(
        self, token_in, token_out, amount_in, fee, pool_address
    ) -> Optional[dict]:
        """Get quote for single-hop swap."""
        try:
            call_data = {
                "method_signature": "quoteExactInputSingle((address,address,uint256,uint24,uint160))",
                "param_types": ["(address,address,uint256,uint24,uint160)"],
                "params": [
                    (
                        Web3.to_checksum_address(token_in),
                        Web3.to_checksum_address(token_out),
                        amount_in,
                        fee,
                        0,
                    )
                ],
            }

            result = self.quoter.batch_eth_call([call_data])

            if isinstance(result, list) and len(result) > 0:
                output_hex = result[0].get("result", "0x")
                if output_hex and output_hex != "0x":
                    amount_out = int(output_hex[:66], 16)
                    return {"amount_out": amount_out}

        except Exception as e:
            logger.warning(f"[SWAP_PATHS] Quote failed for pool {pool_address}: {e}")

        return None

    def _quote_exact_input_multi_hop(
        self, path_tokens, path_fees, amount_in
    ) -> Optional[dict]:
        """Get quote for multi-hop swap."""
        try:
            path_bytes = b""
            for i, token in enumerate(path_tokens):
                token_addr = token[2:] if token.startswith("0x") else token
                path_bytes += bytes.fromhex(token_addr.lower())
                if i < len(path_fees):
                    path_bytes += path_fees[i].to_bytes(3, "big")

            call_data = {
                "method_signature": "quoteExactInput(bytes,uint256)",
                "param_types": ["bytes", "uint256"],
                "params": [path_bytes, amount_in],
            }

            result = self.quoter.batch_eth_call([call_data])

            if isinstance(result, list) and len(result) > 0:
                output_hex = result[0].get("result", "0x")
                if output_hex and output_hex != "0x":
                    amount_out = int(output_hex[:66], 16)
                    return {"amount_out": amount_out}

        except Exception as e:
            logger.warning(f"[SWAP_PATHS] Multi-hop quote failed: {e}")

        return None

    def _update_swap_paths_table(self, paths):
        """
        Update swap paths using temp Log table + atomic swap pattern.

        1. Create temp Log table
        2. Insert data into temp table
        3. Atomically swap temp table with SwapPaths Log table using EXCHANGE TABLES
        4. Drop temp table (which now has old data)

        The ClickHouse dictionary will automatically reload from the new table
        within 1 second (based on LIFETIME(1) setting).
        """
        try:
            temp_table = "SwapPaths_Temp"

            # Step 1: Create temp Log table with same schema
            create_query = f"""
            CREATE TABLE IF NOT EXISTS aave_ethereum.{temp_table}
            (
                token_in String,
                token_out String,
                path String,
                updated_at DateTime DEFAULT now()
            )
            ENGINE = Log;
            """
            self.clickhouse_client.execute_query(create_query)

            # Step 2: Insert data into temp table
            if paths:
                # Use INSERT with explicit column specification to avoid column count mismatch
                # (updated_at has a DEFAULT value so we don't need to provide it)
                # Store lowercase addresses for consistent lookups
                values = []
                for p in paths:
                    token_in = p["token_in"].lower()
                    token_out = p["token_out"].lower()
                    path = p["path"]
                    # Escape single quotes in values
                    token_in_escaped = token_in.replace("'", "\\'")
                    token_out_escaped = token_out.replace("'", "\\'")
                    path_escaped = path.replace("'", "\\'")
                    values.append(
                        f"('{token_in_escaped}', '{token_out_escaped}', '{path_escaped}')"
                    )

                values_str = ", ".join(values)
                insert_query = f"""
                    INSERT INTO aave_ethereum.{temp_table} (token_in, token_out, path)
                    VALUES {values_str}
                """
                self.clickhouse_client.execute_query(insert_query)

            # Step 3: Atomic swap using EXCHANGE TABLES
            # This atomically swaps the table definitions
            self.clickhouse_client.execute_query(
                f"EXCHANGE TABLES aave_ethereum.SwapPaths AND aave_ethereum.{temp_table}"
            )

            # Step 4: Drop the temp table (which now has the old data)
            self.clickhouse_client.execute_query(
                f"DROP TABLE IF EXISTS aave_ethereum.{temp_table}"
            )

            logger.info(
                f"[SWAP_PATHS] Successfully updated SwapPaths table with {len(paths)} paths"
            )

        except Exception as e:
            # Clean up temp table if something went wrong
            try:
                self.clickhouse_client.execute_query(
                    f"DROP TABLE IF EXISTS aave_ethereum.{temp_table}"
                )
            except Exception:
                pass
            raise e


UpdateSwapPathsTask = app.register_task(UpdateSwapPathsTask())

import logging
from typing import Any, Dict, List

from celery import Task
from decouple import config
from eth_abi import encode
from eth_account import Account
from web3 import Web3

from liquidations_v2.celery_app import app
from utils.clickhouse.client import clickhouse_client

logger = logging.getLogger(__name__)


class AaveV3LiquidationPayloadBuilder:
    """
    Builds transaction payloads for Aave V3 liquidations.

    Aave V3 Pool.liquidationCall signature:
    function liquidationCall(
        address collateralAsset,
        address debtAsset,
        address user,
        uint256 debtToCover,
        bool receiveAToken
    ) external;
    """

    AAVE_V3_POOL_ADDRESS = config("POOL_V3_POOL")

    # Aave V3 liquidationCall function selector
    LIQUIDATION_CALL_SELECTOR = Web3.keccak(
        text="liquidationCall(address,address,address,uint256,bool)"
    )[:4].hex()

    def __init__(self):
        self.w3 = Web3()  # Not connected to provider, just for encoding

    def build_liquidation_calldata(
        self,
        collateral_asset: str,
        debt_asset: str,
        user: str,
        debt_to_cover: int,
        receive_atoken: bool = False,
    ) -> str:
        """
        Build the calldata for Aave V3 liquidationCall.

        Args:
            collateral_asset: Address of the collateral asset to receive
            debt_asset: Address of the debt asset to repay
            user: Address of the user to liquidate
            debt_to_cover: Amount of debt to cover (in asset decimals)
            receive_atoken: Whether to receive aTokens instead of underlying

        Returns:
            Hex string of the encoded calldata
        """
        # Encode the function parameters
        encoded_params = encode(
            ["address", "address", "address", "uint256", "bool"],
            [
                Web3.to_checksum_address(collateral_asset),
                Web3.to_checksum_address(debt_asset),
                Web3.to_checksum_address(user),
                debt_to_cover,
                receive_atoken,
            ],
        )

        # Combine selector + encoded params
        calldata = self.LIQUIDATION_CALL_SELECTOR + encoded_params.hex()

        return calldata

    def build_liquidation_transaction(
        self,
        collateral_asset: str,
        debt_asset: str,
        user: str,
        debt_to_cover: int,
        liquidator_address: str,
        nonce: int,
        max_priority_fee_per_gas: int,
        max_fee_per_gas: int,
        gas_limit: int = 500000,
        chain_id: int = 1,
    ) -> Dict[str, Any]:
        """
        Build a complete EIP-1559 transaction for liquidation.

        Args:
            collateral_asset: Address of the collateral asset
            debt_asset: Address of the debt asset
            user: Address of the user to liquidate
            debt_to_cover: Amount of debt to cover
            liquidator_address: Address of the liquidator (transaction sender)
            nonce: Transaction nonce
            max_priority_fee_per_gas: Max priority fee (tip) in wei
            max_fee_per_gas: Max total fee per gas in wei
            gas_limit: Gas limit for the transaction
            chain_id: Chain ID (1 for Ethereum mainnet)

        Returns:
            Dictionary containing the transaction parameters
        """
        calldata = self.build_liquidation_calldata(
            collateral_asset=collateral_asset,
            debt_asset=debt_asset,
            user=user,
            debt_to_cover=debt_to_cover,
            receive_atoken=False,
        )

        transaction = {
            "chainId": chain_id,
            "from": Web3.to_checksum_address(liquidator_address),
            "to": Web3.to_checksum_address(self.AAVE_V3_POOL_ADDRESS),
            "value": 0,
            "nonce": nonce,
            "gas": gas_limit,
            "maxPriorityFeePerGas": max_priority_fee_per_gas,
            "maxFeePerGas": max_fee_per_gas,
            "data": calldata,
            "type": 2,  # EIP-1559 transaction
        }

        return transaction

    def sign_transaction(self, transaction: Dict[str, Any], private_key: str) -> str:
        """
        Sign a transaction with the provided private key.

        Args:
            transaction: Transaction dictionary
            private_key: Private key (with or without 0x prefix)

        Returns:
            Signed raw transaction as hex string
        """
        # Ensure private key has 0x prefix
        if not private_key.startswith("0x"):
            private_key = "0x" + private_key

        account = Account.from_key(private_key)
        signed_txn = account.sign_transaction(transaction)

        return signed_txn.rawTransaction.hex()


class ExecuteLiquidationsTask(Task):
    """
    Main task to execute liquidations after opportunities are detected.

    This task:
    1. Retrieves detected liquidation opportunities from LiquidationDetections table
    2. Prepares transaction payloads for each liquidation
    3. Dispatches transactions to multiple MEV builders in parallel
    4. Logs execution status
    """

    clickhouse_client = clickhouse_client

    def run(self, detection_timestamp: int, updated_assets: List[str]):
        """
        Execute liquidations for detected opportunities.

        Args:
            detection_timestamp: Unix timestamp of when liquidations were detected
            updated_assets: List of assets that triggered the detection
        """
        try:
            logger.info(
                f"[LIQUIDATION_EXECUTION] Starting liquidation execution for "
                f"detection at timestamp {detection_timestamp}"
            )

            # Step 1: Retrieve liquidation opportunities from ClickHouse
            opportunities = self._get_liquidation_opportunities(detection_timestamp)

            if not opportunities:
                logger.warning(
                    f"[LIQUIDATION_EXECUTION] No liquidation opportunities found "
                    f"for timestamp {detection_timestamp}"
                )
                return

            logger.info(
                f"[LIQUIDATION_EXECUTION] Found {len(opportunities)} liquidation "
                f"opportunities to execute"
            )

            # Step 2: Get liquidator configuration
            liquidator_address = config("LIQUIDATOR_ADDRESS")
            private_key = config("LIQUIDATOR_PRIVATE_KEY")
            chain_id = int(config("CHAIN_ID", default="1"))

            # Step 3: Get nonce from RPC (would need to implement this properly)
            # For now, we'll use a placeholder
            base_nonce = self._get_current_nonce(liquidator_address)

            # Step 4: Get current gas prices
            gas_prices = self._get_gas_prices()

            # Step 5: Prepare and submit transactions for each opportunity
            from payments.mev_builders import (
                SubmitToBuildernetTask,
                SubmitToFlashbotsTask,
                SubmitToTitanTask,
            )

            for idx, opportunity in enumerate(opportunities):
                try:
                    logger.info(
                        f"[LIQUIDATION_EXECUTION] Processing opportunity {idx + 1}/{len(opportunities)} | "
                        f"User: {opportunity['user'][:10]}... | Profit: ${opportunity['profit']:,.2f}"
                    )

                    # Build transaction payload
                    payload_builder = AaveV3LiquidationPayloadBuilder()
                    transaction = payload_builder.build_liquidation_transaction(
                        collateral_asset=opportunity["collateral_asset"],
                        debt_asset=opportunity["debt_asset"],
                        user=opportunity["user"],
                        debt_to_cover=int(opportunity["debt_to_cover"]),
                        liquidator_address=liquidator_address,
                        nonce=base_nonce + idx,
                        max_priority_fee_per_gas=gas_prices["priority_fee"],
                        max_fee_per_gas=gas_prices["max_fee"],
                        chain_id=chain_id,
                    )

                    # Sign transaction
                    signed_tx = payload_builder.sign_transaction(
                        transaction, private_key
                    )

                    # Submit to multiple builders in parallel
                    submission_data = {
                        "signed_tx": signed_tx,
                        "transaction": transaction,
                        "opportunity": opportunity,
                        "timestamp": detection_timestamp,
                    }

                    # Fire off to all builders concurrently
                    SubmitToFlashbotsTask.delay(submission_data)
                    SubmitToTitanTask.delay(submission_data)
                    SubmitToBuildernetTask.delay(submission_data)

                    logger.warning(
                        f"[LIQUIDATION_TX_SUBMITTED] Transaction submitted to all builders | "
                        f"User: {opportunity['user'][:10]}... | Nonce: {base_nonce + idx} | "
                        f"Profit: ${opportunity['profit']:,.2f}"
                    )

                except Exception as e:
                    logger.error(
                        f"[LIQUIDATION_EXECUTION_ERROR] Error processing opportunity {idx + 1}: {e}",
                        exc_info=True,
                    )
                    continue

            logger.warning(
                f"[LIQUIDATION_EXECUTION_COMPLETE] Submitted {len(opportunities)} liquidations to builders"
            )

        except Exception as e:
            logger.error(
                f"[LIQUIDATION_EXECUTION_ERROR] Error in ExecuteLiquidationsTask: {e}",
                exc_info=True,
            )

    def _get_liquidation_opportunities(
        self, detection_timestamp: int
    ) -> List[Dict[str, Any]]:
        """
        Retrieve liquidation opportunities from ClickHouse LiquidationDetections table.

        Args:
            detection_timestamp: Unix timestamp to filter by

        Returns:
            List of liquidation opportunities
        """
        query = f"""
        SELECT
            user,
            collateral_asset,
            debt_asset,
            current_health_factor,
            predicted_health_factor,
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
        FROM aave_ethereum.LiquidationDetections
        WHERE detected_at = {detection_timestamp}
        ORDER BY profit DESC
        """

        try:
            result = self.clickhouse_client.execute_query(query)
            opportunities = []

            if result.result_rows:
                for row in result.result_rows:
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
                            "is_priority_debt": int(row[16]),
                            "is_priority_collateral": int(row[17]),
                        }
                    )

            return opportunities

        except Exception as e:
            logger.error(
                f"[LIQUIDATION_EXECUTION_ERROR] Error retrieving opportunities: {e}",
                exc_info=True,
            )
            return []

    def _get_current_nonce(self, address: str) -> int:
        """
        Get the current nonce for the liquidator address.

        TODO: Implement proper RPC call to get nonce

        Args:
            address: Liquidator address

        Returns:
            Current nonce
        """
        # Placeholder - would need to implement RPC call
        from utils.rpc import rpc_adapter

        try:
            nonce = rpc_adapter.w3.eth.get_transaction_count(
                Web3.to_checksum_address(address), "pending"
            )
            return nonce
        except Exception as e:
            logger.error(f"[LIQUIDATION_EXECUTION_ERROR] Error getting nonce: {e}")
            return 0

    def _get_gas_prices(self) -> Dict[str, int]:
        """
        Get current gas prices for transaction submission.

        TODO: Implement proper gas price estimation

        Returns:
            Dictionary with 'priority_fee' and 'max_fee' in wei
        """
        # Placeholder - would need to implement proper gas price oracle
        from utils.rpc import rpc_adapter

        try:
            latest_block = rpc_adapter.w3.eth.get_block("latest")
            base_fee = latest_block.get("baseFeePerGas", 0)

            # Add 20% buffer for priority fee
            priority_fee = int(base_fee * 0.2)
            # Max fee = base fee + priority fee + buffer
            max_fee = int(base_fee * 1.5 + priority_fee)

            return {"priority_fee": priority_fee, "max_fee": max_fee}
        except Exception as e:
            logger.error(f"[LIQUIDATION_EXECUTION_ERROR] Error getting gas prices: {e}")
            # Fallback to reasonable defaults (in gwei)
            return {
                "priority_fee": Web3.to_wei(2, "gwei"),
                "max_fee": Web3.to_wei(50, "gwei"),
            }


ExecuteLiquidationsTask = app.register_task(ExecuteLiquidationsTask())

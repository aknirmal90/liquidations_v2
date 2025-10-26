import logging
from datetime import datetime
from typing import Any, Dict

import requests
from celery import Task
from decouple import config

from liquidations_v2.celery_app import app
from utils.clickhouse.client import clickhouse_client

logger = logging.getLogger(__name__)


class SubmitToFlashbotsTask(Task):
    """
    Submit transactions to Flashbots builder.

    Flashbots Protect RPC endpoint allows transactions to be sent privately
    to Flashbots builders without going through the public mempool.
    """

    # Flashbots RPC endpoints
    FLASHBOTS_RELAY_URL = "https://relay.flashbots.net"
    FLASHBOTS_RPC_URL = config("FLASHBOTS_RPC_URL", default="https://rpc.flashbots.net")

    def run(self, submission_data: Dict[str, Any]):
        """
        Submit a signed transaction to Flashbots.

        Args:
            submission_data: Dictionary containing:
                - signed_tx: Signed raw transaction hex
                - transaction: Transaction dict
                - opportunity: Liquidation opportunity details
                - timestamp: Detection timestamp
        """
        try:
            signed_tx = submission_data["signed_tx"]
            transaction = submission_data["transaction"]
            opportunity = submission_data["opportunity"]

            logger.info(
                f"[FLASHBOTS_SUBMISSION] Submitting transaction to Flashbots | "
                f"User: {opportunity['user'][:10]}... | Nonce: {transaction['nonce']} | "
                f"Profit: ${opportunity['profit']:,.2f}"
            )

            # Prepare Flashbots bundle
            # Note: Flashbots requires bundles, even for single transactions
            bundle = [signed_tx]

            # Get current block number (would need proper RPC implementation)
            target_block = self._get_target_block_number()

            # Submit to Flashbots
            response = self._submit_flashbots_bundle(
                bundle=bundle, target_block=target_block, transaction=transaction
            )

            if response.get("success"):
                logger.warning(
                    f"[FLASHBOTS_SUBMITTED] Successfully submitted to Flashbots | "
                    f"Bundle Hash: {response.get('bundle_hash', 'N/A')} | "
                    f"User: {opportunity['user'][:10]}... | "
                    f"Target Block: {target_block}"
                )

                # Store submission record
                self._store_submission_record(
                    builder="flashbots",
                    submission_data=submission_data,
                    response=response,
                    target_block=target_block,
                )
            else:
                logger.error(
                    f"[FLASHBOTS_ERROR] Failed to submit to Flashbots | "
                    f"Error: {response.get('error', 'Unknown')} | "
                    f"User: {opportunity['user'][:10]}..."
                )

        except Exception as e:
            logger.error(
                f"[FLASHBOTS_ERROR] Error submitting to Flashbots: {e}", exc_info=True
            )

    def _submit_flashbots_bundle(
        self, bundle: list, target_block: int, transaction: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Submit a bundle to Flashbots relay.

        Args:
            bundle: List of signed transaction hex strings
            target_block: Target block number for bundle inclusion
            transaction: Transaction details for logging

        Returns:
            Response dictionary with success status and bundle hash
        """
        try:
            # Flashbots eth_sendBundle params
            params = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_sendBundle",
                "params": [
                    {
                        "txs": bundle,
                        "blockNumber": hex(target_block),
                        "minTimestamp": 0,
                        "maxTimestamp": 0,
                    }
                ],
            }

            # Send to Flashbots RPC
            response = requests.post(
                self.FLASHBOTS_RPC_URL,
                json=params,
                headers={
                    "Content-Type": "application/json",
                    "X-Flashbots-Signature": self._sign_flashbots_request(params),
                },
                timeout=10,
            )

            if response.status_code == 200:
                result = response.json()
                if "result" in result:
                    return {
                        "success": True,
                        "bundle_hash": result["result"].get("bundleHash"),
                        "response": result,
                    }
                else:
                    return {
                        "success": False,
                        "error": result.get("error", {}).get(
                            "message", "Unknown error"
                        ),
                    }
            else:
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text}",
                }

        except Exception as e:
            logger.error(f"[FLASHBOTS_ERROR] Exception in bundle submission: {e}")
            return {"success": False, "error": str(e)}

    def _sign_flashbots_request(self, params: Dict[str, Any]) -> str:
        """
        Sign a Flashbots request with the Flashbots signature key.

        Args:
            params: Request parameters to sign

        Returns:
            Signature string for X-Flashbots-Signature header
        """
        # TODO: Implement proper Flashbots signature
        # For now, return placeholder
        # flashbots_key = config("FLASHBOTS_SIGNATURE_KEY", default="")
        return f"{config('LIQUIDATOR_ADDRESS', default='')}:signature_placeholder"

    def _get_target_block_number(self) -> int:
        """Get the target block number for bundle submission."""
        from utils.rpc import rpc_adapter

        try:
            current_block = rpc_adapter.w3.eth.block_number
            # Target next block
            return current_block + 1
        except Exception as e:
            logger.error(f"[FLASHBOTS_ERROR] Error getting block number: {e}")
            return 0

    def _store_submission_record(
        self,
        builder: str,
        submission_data: Dict[str, Any],
        response: Dict[str, Any],
        target_block: int,
    ):
        """Store MEV submission record in ClickHouse."""
        try:
            opportunity = submission_data["opportunity"]
            transaction = submission_data["transaction"]

            record = [
                builder,  # builder_name
                opportunity["user"],  # user
                opportunity["collateral_asset"],  # collateral_asset
                opportunity["debt_asset"],  # debt_asset
                opportunity["profit"],  # expected_profit
                transaction["nonce"],  # nonce
                target_block,  # target_block
                response.get("bundle_hash", ""),  # bundle_hash
                response.get("tx_hash", ""),  # tx_hash
                1 if response.get("success") else 0,  # submission_success
                response.get("error", ""),  # error_message
                int(datetime.now().timestamp()),  # submitted_at
            ]

            clickhouse_client.insert_rows("LiquidationSubmissions", [record])

            logger.info(
                f"[FLASHBOTS_SUBMISSION] Stored submission record for builder: {builder}"
            )

        except Exception as e:
            logger.error(f"[FLASHBOTS_ERROR] Error storing submission record: {e}")


class SubmitToTitanTask(Task):
    """
    Submit transactions to Titan builder.

    Titan is an MEV builder that provides competitive transaction inclusion.
    """

    TITAN_RPC_URL = config("TITAN_RPC_URL", default="https://rpc.titanbuilder.xyz")

    def run(self, submission_data: Dict[str, Any]):
        """
        Submit a signed transaction to Titan builder.

        Args:
            submission_data: Dictionary containing transaction and opportunity data
        """
        try:
            signed_tx = submission_data["signed_tx"]
            transaction = submission_data["transaction"]
            opportunity = submission_data["opportunity"]
            # timestamp = submission_data["timestamp"]

            logger.info(
                f"[TITAN_SUBMISSION] Submitting transaction to Titan | "
                f"User: {opportunity['user'][:10]}... | Nonce: {transaction['nonce']} | "
                f"Profit: ${opportunity['profit']:,.2f}"
            )

            # Submit to Titan builder
            response = self._submit_to_titan(signed_tx, transaction)

            if response.get("success"):
                logger.warning(
                    f"[TITAN_SUBMITTED] Successfully submitted to Titan | "
                    f"TX Hash: {response.get('tx_hash', 'N/A')} | "
                    f"User: {opportunity['user'][:10]}..."
                )

                # Store submission record
                self._store_submission_record(
                    builder="titan", submission_data=submission_data, response=response
                )
            else:
                logger.error(
                    f"[TITAN_ERROR] Failed to submit to Titan | "
                    f"Error: {response.get('error', 'Unknown')} | "
                    f"User: {opportunity['user'][:10]}..."
                )

        except Exception as e:
            logger.error(f"[TITAN_ERROR] Error submitting to Titan: {e}", exc_info=True)

    def _submit_to_titan(
        self, signed_tx: str, transaction: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Submit a signed transaction to Titan RPC.

        Args:
            signed_tx: Signed raw transaction hex
            transaction: Transaction details

        Returns:
            Response dictionary with success status
        """
        try:
            # Titan uses standard eth_sendRawTransaction
            params = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_sendRawTransaction",
                "params": [signed_tx],
            }

            response = requests.post(
                self.TITAN_RPC_URL,
                json=params,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )

            if response.status_code == 200:
                result = response.json()
                if "result" in result:
                    return {
                        "success": True,
                        "tx_hash": result["result"],
                        "response": result,
                    }
                else:
                    return {
                        "success": False,
                        "error": result.get("error", {}).get(
                            "message", "Unknown error"
                        ),
                    }
            else:
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text}",
                }

        except Exception as e:
            logger.error(f"[TITAN_ERROR] Exception in submission: {e}")
            return {"success": False, "error": str(e)}

    def _store_submission_record(
        self, builder: str, submission_data: Dict[str, Any], response: Dict[str, Any]
    ):
        """Store MEV submission record."""
        try:
            opportunity = submission_data["opportunity"]
            transaction = submission_data["transaction"]

            record = [
                builder,  # builder_name
                opportunity["user"],  # user
                opportunity["collateral_asset"],  # collateral_asset
                opportunity["debt_asset"],  # debt_asset
                opportunity["profit"],  # expected_profit
                transaction["nonce"],  # nonce
                0,  # target_block (not applicable for direct RPC)
                "",  # bundle_hash (not applicable)
                response.get("tx_hash", ""),  # tx_hash
                1 if response.get("success") else 0,  # submission_success
                response.get("error", ""),  # error_message
                int(datetime.now().timestamp()),  # submitted_at
            ]

            clickhouse_client.insert_rows("LiquidationSubmissions", [record])

            logger.info(
                f"[TITAN_SUBMISSION] Recorded submission for builder: {builder}"
            )

        except Exception as e:
            logger.error(f"[TITAN_ERROR] Error storing submission record: {e}")


class SubmitToBuildernetTask(Task):
    """
    Submit transactions to BuilderNet (formerly known as Builder0x69).

    BuilderNet is a decentralized builder network for MEV transactions.
    """

    BUILDERNET_RPC_URL = config(
        "BUILDERNET_RPC_URL", default="https://rpc.beaverbuild.org"
    )

    def run(self, submission_data: Dict[str, Any]):
        """
        Submit a signed transaction to BuilderNet.

        Args:
            submission_data: Dictionary containing transaction and opportunity data
        """
        try:
            signed_tx = submission_data["signed_tx"]
            transaction = submission_data["transaction"]
            opportunity = submission_data["opportunity"]
            # timestamp = submission_data["timestamp"]

            logger.info(
                f"[BUILDERNET_SUBMISSION] Submitting transaction to BuilderNet | "
                f"User: {opportunity['user'][:10]}... | Nonce: {transaction['nonce']} | "
                f"Profit: ${opportunity['profit']:,.2f}"
            )

            # Submit to BuilderNet
            response = self._submit_to_buildernet(signed_tx, transaction)

            if response.get("success"):
                logger.warning(
                    f"[BUILDERNET_SUBMITTED] Successfully submitted to BuilderNet | "
                    f"TX Hash: {response.get('tx_hash', 'N/A')} | "
                    f"User: {opportunity['user'][:10]}..."
                )

                # Store submission record
                self._store_submission_record(
                    builder="buildernet",
                    submission_data=submission_data,
                    response=response,
                )
            else:
                logger.error(
                    f"[BUILDERNET_ERROR] Failed to submit to BuilderNet | "
                    f"Error: {response.get('error', 'Unknown')} | "
                    f"User: {opportunity['user'][:10]}..."
                )

        except Exception as e:
            logger.error(
                f"[BUILDERNET_ERROR] Error submitting to BuilderNet: {e}", exc_info=True
            )

    def _submit_to_buildernet(
        self, signed_tx: str, transaction: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Submit a signed transaction to BuilderNet RPC.

        Args:
            signed_tx: Signed raw transaction hex
            transaction: Transaction details

        Returns:
            Response dictionary with success status
        """
        try:
            # BuilderNet uses standard eth_sendRawTransaction
            params = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_sendRawTransaction",
                "params": [signed_tx],
            }

            response = requests.post(
                self.BUILDERNET_RPC_URL,
                json=params,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )

            if response.status_code == 200:
                result = response.json()
                if "result" in result:
                    return {
                        "success": True,
                        "tx_hash": result["result"],
                        "response": result,
                    }
                else:
                    return {
                        "success": False,
                        "error": result.get("error", {}).get(
                            "message", "Unknown error"
                        ),
                    }
            else:
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text}",
                }

        except Exception as e:
            logger.error(f"[BUILDERNET_ERROR] Exception in submission: {e}")
            return {"success": False, "error": str(e)}

    def _store_submission_record(
        self, builder: str, submission_data: Dict[str, Any], response: Dict[str, Any]
    ):
        """Store MEV submission record."""
        try:
            opportunity = submission_data["opportunity"]
            transaction = submission_data["transaction"]

            record = [
                builder,  # builder_name
                opportunity["user"],  # user
                opportunity["collateral_asset"],  # collateral_asset
                opportunity["debt_asset"],  # debt_asset
                opportunity["profit"],  # expected_profit
                transaction["nonce"],  # nonce
                0,  # target_block (not applicable for direct RPC)
                "",  # bundle_hash (not applicable)
                response.get("tx_hash", ""),  # tx_hash
                1 if response.get("success") else 0,  # submission_success
                response.get("error", ""),  # error_message
                int(datetime.now().timestamp()),  # submitted_at
            ]

            clickhouse_client.insert_rows("LiquidationSubmissions", [record])

            logger.info(
                f"[BUILDERNET_SUBMISSION] Recorded submission for builder: {builder}"
            )

        except Exception as e:
            logger.error(f"[BUILDERNET_ERROR] Error storing submission record: {e}")


# Register tasks with Celery
SubmitToFlashbotsTask = app.register_task(SubmitToFlashbotsTask())
SubmitToTitanTask = app.register_task(SubmitToTitanTask())
SubmitToBuildernetTask = app.register_task(SubmitToBuildernetTask())

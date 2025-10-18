from typing import Dict, List

from decouple import config

from utils.interfaces.base import BaseContractInterface


class PoolInterface(BaseContractInterface):
    def __init__(self):
        pool_address = config("POOL_V3_POOL")
        super().__init__(pool_address)
        # Keep backward compatibility alias
        self.pool_address = self.contract_address

    def get_user_emode(self, users: List[str]) -> Dict[str, int]:
        """
        Issues a batch of getUserEMode(address) calls and decodes the results.
        Returns a dict mapping user address to eMode category ID.

        Args:
            users: List of user addresses to query

        Returns:
            Dictionary mapping user address to eMode category ID (uint256)
        """
        call_targets = [
            {
                "method_signature": "getUserEMode(address)",
                "param_types": ["address"],
                "params": [user],
            }
            for user in users
        ]
        raw_results = self.batch_eth_call(call_targets)

        def decode_result(hex_result):
            return self.decode_eth_call_result(hex_result, "getUserEMode")

        results_by_user = {}
        if isinstance(raw_results, list):
            for user, rpc_result in zip(users, raw_results):
                result_value = rpc_result.get("result", "")
                results_by_user[user] = decode_result(result_value)
            return results_by_user
        return raw_results

    def get_user_account_data(self, users: List[str]) -> Dict[str, Dict]:
        """
        Issues a batch of getUserAccountData(address) calls and decodes the results.
        Returns a dict mapping user address to account data.
        """
        call_targets = [
            {
                "method_signature": "getUserAccountData(address)",
                "param_types": ["address"],
                "params": [user],
            }
            for user in users
        ]
        raw_results = self.batch_eth_call(call_targets)

        def decode_result(hex_result):
            return self.decode_eth_call_result(hex_result, "getUserAccountData")

        results_by_user = {}
        if isinstance(raw_results, list):
            for user, rpc_result in zip(users, raw_results):
                result_value = rpc_result.get("result", "")
                results_by_user[user] = decode_result(result_value)
            return results_by_user
        return raw_results

    def get_reserve_data(self, reserves: List[str]) -> Dict[str, Dict]:
        """
        Issues a batch of getReserveData(address) calls and decodes the results.
        Returns a dict mapping reserve address to reserve data.

        Decodes the Aave V3 ReserveData struct manually:
        struct ReserveData {
            ReserveConfigurationMap configuration;  // uint256
            uint128 liquidityIndex;
            uint128 currentLiquidityRate;
            uint128 variableBorrowIndex;
            uint128 currentVariableBorrowRate;
            uint128 currentStableBorrowRate;
            uint40 lastUpdateTimestamp;
            uint16 id;
            uint40 liquidationGracePeriodUntil;
            address aTokenAddress;
            address stableDebtTokenAddress;
            address variableDebtTokenAddress;
            address interestRateStrategyAddress;
            uint128 accruedToTreasury;
            uint128 unbacked;
            uint128 isolationModeTotalDebt;
            uint128 virtualUnderlyingBalance;
        }
        """
        call_targets = [
            {
                "method_signature": "getReserveData(address)",
                "param_types": ["address"],
                "params": [reserve],
            }
            for reserve in reserves
        ]
        raw_results = self.batch_eth_call(call_targets)

        def decode_result(hex_result):
            if not hex_result or hex_result == "0x":
                return None

            # Remove "0x" prefix
            if hex_result.startswith("0x"):
                hex_result = hex_result[2:]

            data = bytes.fromhex(hex_result)

            # Manual decoding to handle Solidity struct packing
            # Each value is stored in 32-byte slots in the return data
            try:
                offset = 0

                # configuration (uint256) - 32 bytes
                configuration = int.from_bytes(data[offset : offset + 32], "big")
                offset += 32

                # liquidityIndex (uint128) - stored in 32 bytes (right-padded with zeros)
                liquidityIndex = int.from_bytes(data[offset : offset + 32], "big")
                offset += 32

                # currentLiquidityRate (uint128)
                currentLiquidityRate = int.from_bytes(data[offset : offset + 32], "big")
                offset += 32

                # variableBorrowIndex (uint128)
                variableBorrowIndex = int.from_bytes(data[offset : offset + 32], "big")
                offset += 32

                # currentVariableBorrowRate (uint128)
                currentVariableBorrowRate = int.from_bytes(
                    data[offset : offset + 32], "big"
                )
                offset += 32

                # currentStableBorrowRate (uint128)
                currentStableBorrowRate = int.from_bytes(
                    data[offset : offset + 32], "big"
                )
                offset += 32

                # lastUpdateTimestamp (uint40), id (uint16), liquidationGracePeriodUntil (uint40)
                # These are packed together in one 32-byte slot
                packed = int.from_bytes(data[offset : offset + 32], "big")
                offset += 32

                # Extract from the packed value (reading right to left)
                liquidationGracePeriodUntil = (packed >> 16) & ((1 << 40) - 1)
                id_val = packed & ((1 << 16) - 1)
                lastUpdateTimestamp = (packed >> 56) & ((1 << 40) - 1)

                # aTokenAddress (address) - 20 bytes in 32-byte slot
                aTokenAddress = "0x" + data[offset + 12 : offset + 32].hex()
                offset += 32

                # stableDebtTokenAddress (address)
                stableDebtTokenAddress = "0x" + data[offset + 12 : offset + 32].hex()
                offset += 32

                # variableDebtTokenAddress (address)
                variableDebtTokenAddress = "0x" + data[offset + 12 : offset + 32].hex()
                offset += 32

                # interestRateStrategyAddress (address)
                interestRateStrategyAddress = (
                    "0x" + data[offset + 12 : offset + 32].hex()
                )
                offset += 32

                # accruedToTreasury (uint128)
                accruedToTreasury = int.from_bytes(data[offset : offset + 32], "big")
                offset += 32

                # unbacked (uint128)
                unbacked = int.from_bytes(data[offset : offset + 32], "big")
                offset += 32

                # isolationModeTotalDebt (uint128)
                isolationModeTotalDebt = int.from_bytes(
                    data[offset : offset + 32], "big"
                )
                offset += 32

                # virtualUnderlyingBalance (uint128)
                virtualUnderlyingBalance = int.from_bytes(
                    data[offset : offset + 32], "big"
                )

                return {
                    "configuration": configuration,
                    "liquidityIndex": liquidityIndex,
                    "currentLiquidityRate": currentLiquidityRate,
                    "variableBorrowIndex": variableBorrowIndex,
                    "currentVariableBorrowRate": currentVariableBorrowRate,
                    "currentStableBorrowRate": currentStableBorrowRate,
                    "lastUpdateTimestamp": lastUpdateTimestamp,
                    "id": id_val,
                    "liquidationGracePeriodUntil": liquidationGracePeriodUntil,
                    "aTokenAddress": aTokenAddress,
                    "stableDebtTokenAddress": stableDebtTokenAddress,
                    "variableDebtTokenAddress": variableDebtTokenAddress,
                    "interestRateStrategyAddress": interestRateStrategyAddress,
                    "accruedToTreasury": accruedToTreasury,
                    "unbacked": unbacked,
                    "isolationModeTotalDebt": isolationModeTotalDebt,
                    "virtualUnderlyingBalance": virtualUnderlyingBalance,
                }
            except Exception as e:
                print(f"Error decoding reserve data for result: {e}")
                return None

        results_by_reserve = {}
        if isinstance(raw_results, list):
            for reserve, rpc_result in zip(reserves, raw_results):
                result_value = rpc_result.get("result", "")
                decoded_data = decode_result(result_value)
                if decoded_data:
                    results_by_reserve[reserve] = decoded_data
            return results_by_reserve
        return raw_results

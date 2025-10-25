#!/usr/bin/env python3
"""
Generate Solidity test functions from samples.csv
Each liquidation gets its own test function that forks at the correct block number
"""

import csv
import sys


def address_checksum(addr):
    """Convert address to EIP-55 checksum format using Foundry's cast"""
    import subprocess

    try:
        result = subprocess.run(
            ["cast", "to-check-sum-address", addr],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip().split("\n")[
            0
        ]  # Get first line (address), ignore warnings
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Fallback: return as-is if cast is not available
        return addr


def parse_path(path_str):
    """Parse Uniswap path string"""
    if path_str in ["NO_PATH", "N/A", ""]:
        return "NO_PATH"
    # Return the pool address from CSV
    return path_str


def generate_test_function(row, index):
    """Generate a Solidity test function for a single liquidation"""
    user = row["user"]
    collateral = row["collateral_asset"]
    debt = row["debt_asset"]
    debt_to_cover = row["debt_to_cover"]
    collateral_amount = row["liquidated_collateral_amount"]
    block_height = row["block_height"]
    tx_hash = row["transaction_hash"]
    tx_index = row["transaction_index"]
    path_to_debt = parse_path(row["path_collateral_to_debt"])
    path_to_weth = parse_path(row["path_collateral_to_weth"])

    # Convert addresses to checksum format
    user = address_checksum(user)
    collateral = address_checksum(collateral)
    debt = address_checksum(debt)

    test_name = f"testLiquidation_{index:04d}_Block{block_height}"

    return f'''
    /// @notice Test liquidation #{index} at block {block_height}, tx index {tx_index}
    /// @dev User: {user}, Debt: {debt_to_cover}, Block: {block_height}
    function {test_name}() public {{
        LiquidationData memory liq = LiquidationData({{
            user: {user},
            collateralAsset: {collateral},
            debtAsset: {debt},
            debtToCover: {debt_to_cover},
            liquidatedCollateralAmount: {collateral_amount},
            txHash: {tx_hash},
            txIndex: {tx_index},
            blockHeight: {block_height},
            originalLiquidator: address(0),
            pathCollateralToDebt: "{path_to_debt}",
            pathCollateralToWeth: "{path_to_weth}"
        }});

        executeLiquidationTest(liq);
    }}
'''


def generate_full_test_file(csv_path, max_tests=50, specific_indices=None):
    """Generate complete Solidity test file"""

    header = """// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "forge-std/Test.sol";
import "../src/liquidator.sol";

/// @notice Generated tests for historical liquidations
/// @dev Each test forks at a specific block and attempts the liquidation
contract LiquidatorHistoricalTest is Test {
    AaveV3MEVLiquidator public liquidator;
    address constant OWNER = 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266;

    // Token addresses
    address constant USDC = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48;
    address constant USDT = 0xdAC17F958D2ee523a2206206994597C13D831ec7;
    address constant DAI = 0x6B175474E89094C44Da98b954EedeAC495271d0F;
    address constant WETH = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2;
    address constant WBTC = 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599;

    struct LiquidationData {
        address user;
        address collateralAsset;
        address debtAsset;
        uint256 debtToCover;
        uint256 liquidatedCollateralAmount;
        bytes32 txHash;
        uint256 txIndex;
        uint256 blockHeight;
        address originalLiquidator;
        string pathCollateralToDebt;
        string pathCollateralToWeth;
    }

    function deployLiquidatorAtBlock(uint256 blockNumber) internal {
        vm.createSelectFork("https://reth-ethereum.ithaca.xyz/rpc", blockNumber - 1);

        vm.startPrank(OWNER);
        liquidator = new AaveV3MEVLiquidator();

        liquidator.addWhitelistedDebtAsset(USDC);
        liquidator.addWhitelistedDebtAsset(USDT);
        liquidator.addWhitelistedDebtAsset(DAI);
        liquidator.addWhitelistedDebtAsset(WETH);
        liquidator.addWhitelistedDebtAsset(WBTC);

        liquidator.addWhitelistedCollateralAsset(WETH);
        liquidator.addWhitelistedCollateralAsset(WBTC);
        liquidator.addWhitelistedCollateralAsset(USDC);
        liquidator.addWhitelistedCollateralAsset(USDT);

        liquidator.addWhitelistedLiquidator(OWNER);
        vm.stopPrank();
    }

    function executeLiquidationTest(LiquidationData memory liq) internal {
        deployLiquidatorAtBlock(liq.blockHeight);

        console.log("Block:", liq.blockHeight);
        console.log("User:", liq.user);
        console.log("Debt to cover:", liq.debtToCover);

        AaveV3MEVLiquidator.LiquidationParams[] memory params =
            new AaveV3MEVLiquidator.LiquidationParams[](1);

        params[0] = AaveV3MEVLiquidator.LiquidationParams({
            user: liq.user,
            debtAsset: liq.debtAsset,
            collateralAsset: liq.collateralAsset,
            debtToCover: liq.debtToCover
        });

        // Prepare swap paths
        AaveV3MEVLiquidator.SwapPath[] memory swapPaths =
            new AaveV3MEVLiquidator.SwapPath[](1);

        bytes memory pathToDebt = parseUniswapPath(
            liq.pathCollateralToDebt,
            liq.collateralAsset,
            liq.debtAsset
        );

        bytes memory pathToWeth = parseUniswapPath(
            liq.pathCollateralToWeth,
            liq.collateralAsset,
            WETH
        );

        swapPaths[0] = AaveV3MEVLiquidator.SwapPath({
            pathCollateralToDebt: reverseUniswapPath(pathToDebt),
            pathCollateralToWETH: pathToWeth,
            maxCollateralForRepayment: (liq.liquidatedCollateralAmount * 105) / 100
        });

        // Replay prior transaction (oracle update) if needed
        if (liq.txIndex > 0) {
            console.log("Replaying prior transaction at index:", liq.txIndex - 1);
            replayPriorTransaction(liq.blockHeight, liq.txIndex);
        }

        // Log balance before liquidation
        uint256 balanceBefore = OWNER.balance;
        console.log("Liquidator balance before:", balanceBefore);

        vm.prank(OWNER);
        try liquidator.executeLiquidations(params, swapPaths, liq.debtToCover, 90) {
            uint256 balanceAfter = OWNER.balance;
            console.log("Liquidator balance after:", balanceAfter);

            if (balanceAfter > balanceBefore) {
                console.log("Profit (ETH):", balanceAfter - balanceBefore);
                console.log("Profit (Wei):", balanceAfter - balanceBefore);
            } else if (balanceAfter < balanceBefore) {
                console.log("Loss (Wei):", balanceBefore - balanceAfter);
            } else {
                console.log("No profit/loss");
            }
            console.log("SUCCESS - Liquidation executed");
        } catch Error(string memory reason) {
            console.log("FAILED:", reason);
        } catch (bytes memory) {
            console.log("FAILED: Low-level error");
        }
    }

    /// @notice Replay the transaction that occurred before the liquidation (usually oracle update)
    /// @param blockNumber The block number containing the transaction
    /// @param liquidationTxIndex The index of the liquidation transaction
    function replayPriorTransaction(uint256 blockNumber, uint256 liquidationTxIndex) internal {
        if (liquidationTxIndex == 0) {
            console.log("No prior transaction to replay (txIndex = 0)");
            return;
        }

        uint256 priorTxIndex = liquidationTxIndex - 1;

        // Get transaction JSON from shell script
        string[] memory inputs = new string[](4);
        inputs[0] = "bash";
        inputs[1] = "scripts/get_tx.sh";
        inputs[2] = vm.toString(blockNumber);
        inputs[3] = vm.toString(priorTxIndex);

        try vm.ffi(inputs) returns (bytes memory result) {
            string memory txJson = string(result);

            // Parse JSON to get transaction details
            string memory txFrom = vm.parseJsonString(txJson, ".from");
            string memory txTo = vm.parseJsonString(txJson, ".to");
            string memory txInput = vm.parseJsonString(txJson, ".input");
            string memory txValue = vm.parseJsonString(txJson, ".value");

            console.log("Replaying transaction from:", txFrom);
            console.log("Replaying transaction to:", txTo);

            // Convert string addresses to address type
            address fromAddr = vm.parseAddress(txFrom);
            address toAddr = vm.parseAddress(txTo);
            bytes memory txData = vm.parseBytes(txInput);
            uint256 value = vm.parseUint(txValue);

            // Replay the transaction
            vm.prank(fromAddr);
            (bool success, ) = toAddr.call{value: value}(txData);

            if (success) {
                console.log("Successfully replayed prior transaction");
            } else {
                console.log("Warning: Prior transaction replay failed (may be expected)");
            }
        } catch {
            console.log("Warning: Could not fetch/replay prior transaction");
        }
    }

    function parseUniswapPath(string memory pathStr, address tokenIn, address tokenOut)
        internal pure returns (bytes memory)
    {
        if (
            keccak256(bytes(pathStr)) == keccak256(bytes("NO_PATH")) ||
            keccak256(bytes(pathStr)) == keccak256(bytes("N/A")) ||
            bytes(pathStr).length == 0
        ) {
            return "";
        }

        uint24 fee = 3000;
        if ((tokenIn == WETH && (tokenOut == USDC || tokenOut == USDT)) ||
            (tokenOut == WETH && (tokenIn == USDC || tokenIn == USDT))) {
            fee = 500;
        }

        return abi.encodePacked(tokenIn, fee, tokenOut);
    }

    function reverseUniswapPath(bytes memory path) internal pure returns (bytes memory) {
        if (path.length == 0) return path;
        if (path.length == 43) {
            address tokenIn;
            uint24 fee;
            address tokenOut;
            assembly {
                tokenIn := mload(add(path, 20))
                fee := mload(add(path, 23))
                tokenOut := mload(add(path, 43))
            }
            return abi.encodePacked(tokenOut, fee, tokenIn);
        }
        return path;
    }

    receive() external payable {}
"""

    footer = "\n}\n"

    # Read CSV and generate tests
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        tests = []

        if specific_indices:
            # Generate tests for specific indices only
            all_rows = list(reader)
            for idx in specific_indices:
                if 1 <= idx <= len(all_rows):
                    row = all_rows[idx - 1]  # Convert 1-based to 0-based
                    test_func = generate_test_function(row, idx)
                    tests.append(test_func)
        else:
            # Generate tests up to max_tests
            for i, row in enumerate(reader):
                if i >= max_tests:
                    break
                test_func = generate_test_function(row, i + 1)
                tests.append(test_func)

    return header + "\n".join(tests) + footer


if __name__ == "__main__":
    csv_path = "samples.csv"

    # Check if we're piping indices from stdin
    if not sys.stdin.isatty():
        # Read indices from stdin (one per line)
        indices_str = sys.stdin.read().strip()
        specific_indices = [int(x) for x in indices_str.split("\n") if x.strip()]
        print(f"Generating {len(specific_indices)} tests from specific indices...")
        test_file = generate_full_test_file(csv_path, specific_indices=specific_indices)
    else:
        max_tests = 10 if len(sys.argv) < 2 else int(sys.argv[1])
        print(f"Generating tests from {csv_path} (max {max_tests} tests)...")
        test_file = generate_full_test_file(csv_path, max_tests)

    output_path = "test/LiquidatorHistorical.t.sol"
    with open(output_path, "w") as f:
        f.write(test_file)

    num_tests = len(specific_indices) if not sys.stdin.isatty() else max_tests
    print(f"Generated {output_path} with {num_tests} test functions")

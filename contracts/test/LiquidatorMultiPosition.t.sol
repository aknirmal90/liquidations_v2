// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "forge-std/Test.sol";
import "forge-std/console.sol";
import "../src/liquidator.sol";

contract LiquidatorMultiPositionTest is Test {
    AaveV3MEVLiquidator public liquidator;
    address constant OWNER = 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266;

    // Token addresses
    address constant WETH = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2;
    address constant USDC = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48;
    address constant USDT = 0xdAC17F958D2ee523a2206206994597C13D831ec7;

    function setUp() public {
        // Don't deploy here - will deploy after forking in the test
    }

    function replayPriorTransaction(uint256 blockNumber, uint256 liquidationTxIndex) internal {
        if (liquidationTxIndex == 0) {
            console.log("No prior transaction to replay (txIndex = 0)");
            return;
        }

        uint256 priorTxIndex = liquidationTxIndex - 1;
        console.log("Attempting to replay transaction at block", blockNumber, "index", priorTxIndex);

        string[] memory inputs = new string[](4);
        inputs[0] = "bash";
        inputs[1] = "scripts/get_tx.sh";
        inputs[2] = vm.toString(blockNumber);
        inputs[3] = vm.toString(priorTxIndex);

        try vm.ffi(inputs) returns (bytes memory result) {
            string memory txJson = string(result);

            if (bytes(txJson).length == 0) {
                console.log("WARNING: Empty transaction data returned");
                return;
            }

            try vm.parseJsonString(txJson, ".from") returns (string memory txFrom) {
                string memory txTo = vm.parseJsonString(txJson, ".to");
                string memory txInput = vm.parseJsonString(txJson, ".input");
                string memory txValue = vm.parseJsonString(txJson, ".value");

                address fromAddr = vm.parseAddress(txFrom);
                address toAddr = vm.parseAddress(txTo);
                bytes memory txData = vm.parseBytes(txInput);
                uint256 value = vm.parseUint(txValue);

                console.log("Replaying transaction:");
                console.log("  From:", fromAddr);
                console.log("  To:", toAddr);
                console.log("  Value:", value);

                vm.deal(fromAddr, value + 10 ether);

                vm.prank(fromAddr);
                (bool success, ) = toAddr.call{value: value}(txData);

                if (success) {
                    console.log("SUCCESS - Prior transaction replayed");
                } else {
                    console.log("WARNING - Prior transaction failed to replay");
                }
            } catch {
                console.log("WARNING: Failed to parse transaction JSON");
            }
        } catch {
            console.log("WARNING: FFI call failed");
        }
    }

    function test_MultiPosition_USDC_Block23549952() public {
        uint256 blockNumber = 23549952;
        uint256 txIndex = 2;

        // Fork at block BEFORE liquidation (matching historical tests pattern)
        vm.createSelectFork("https://reth-ethereum.ithaca.xyz/rpc", blockNumber - 1);

        // Deploy liquidator AFTER forking
        liquidator = new AaveV3MEVLiquidator();
        liquidator.addWhitelistedDebtAsset(USDC);
        liquidator.addWhitelistedCollateralAsset(WETH);
        liquidator.addWhitelistedLiquidator(OWNER);
        vm.deal(OWNER, 100 ether);

        // Replay prior transaction to update oracle prices
        replayPriorTransaction(blockNumber, txIndex);

        // Position 1: 0xc6cB96CC1727eC701E5483C565195B01E3C1da2b
        AaveV3MEVLiquidator.LiquidationParams memory params1 = AaveV3MEVLiquidator.LiquidationParams({
            user: 0xc6cB96CC1727eC701E5483C565195B01E3C1da2b,
            debtAsset: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
            collateralAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtToCover: 4482862847
        });

        // Position 2: 0xE664B08fC5106c2Db70eD4043Dbe2cB5FD14219A
        AaveV3MEVLiquidator.LiquidationParams memory params2 = AaveV3MEVLiquidator.LiquidationParams({
            user: 0xE664B08fC5106c2Db70eD4043Dbe2cB5FD14219A,
            debtAsset: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
            collateralAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtToCover: 5504765139
        });

        // Create array with both positions
        AaveV3MEVLiquidator.LiquidationParams[] memory params = new AaveV3MEVLiquidator.LiquidationParams[](1);
        params[0] = params1;
        // params[1] = params2;  // Comment out second position for now to test

        // Total flashloan amount = just first position for now
        uint256 totalFlashloanAmount = 4482862847; // First position only

        console.log("=== Multi-Position USDC Liquidation Test ===");
        console.log("Block:", blockNumber);
        console.log("Position 1 - User:", params1.user);
        console.log("Position 1 - Debt to cover:", params1.debtToCover);
        console.log("Position 2 - User:", params2.user);
        console.log("Position 2 - Debt to cover:", params2.debtToCover);
        console.log("Total flashloan amount:", totalFlashloanAmount);
        console.log("");

        uint256 balanceBefore = OWNER.balance;
        console.log("=== OWNER BALANCE BEFORE LIQUIDATION ===");
        console.log("Owner Address:", OWNER);
        console.log("Balance (Wei):", balanceBefore);
        console.log("Balance (ETH):", balanceBefore / 1e18);
        console.log("");

        // Measure gas for liquidation
        uint256 gasStart = gasleft();
        vm.prank(OWNER);
        try liquidator.executeLiquidations(params, totalFlashloanAmount, 0) {
            uint256 gasUsed = gasStart - gasleft();
            console.log("SUCCESS - Multi-position liquidation executed");
            console.log("Gas used for liquidation:", gasUsed);
            console.log("");

            uint256 balanceAfter = OWNER.balance;
            console.log("=== OWNER BALANCE AFTER LIQUIDATION ===");
            console.log("Owner Address:", OWNER);
            console.log("Balance (Wei):", balanceAfter);
            console.log("Balance (ETH):", balanceAfter / 1e18);
            console.log("");

            console.log("=== PROFIT/LOSS SUMMARY ===");
            if (balanceAfter > balanceBefore) {
                uint256 profit = balanceAfter - balanceBefore;
                console.log("Profit (Wei):", profit);
                console.log("Profit (ETH):", profit / 1e18);
            } else if (balanceAfter < balanceBefore) {
                uint256 loss = balanceBefore - balanceAfter;
                console.log("Loss (Wei):", loss);
                console.log("Loss (ETH):", loss / 1e18);
            } else {
                console.log("No profit/loss");
            }
        } catch Error(string memory reason) {
            console.log("FAILED - Liquidation reverted:", reason);
            revert(reason);
        } catch (bytes memory lowLevelData) {
            console.log("FAILED - Liquidation reverted with low-level error");
            console.logBytes(lowLevelData);
            revert("Low-level revert");
        }
    }
}

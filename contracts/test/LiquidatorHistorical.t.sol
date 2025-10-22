// SPDX-License-Identifier: MIT
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

    function _logAavePosition(address user, string memory label) internal view {
        IAaveV3Pool aavePool = IAaveV3Pool(
            0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2
        );
        (
            uint256 totalCollateral,
            uint256 totalDebt,
            ,
            ,
            ,
            uint256 healthFactor
        ) = aavePool.getUserAccountData(user);

        console.log("");
        console.log(
            string(
                abi.encodePacked(
                    "=== USER AAVE POSITION ",
                    label,
                    " LIQUIDATION ==="
                )
            )
        );
        console.log("User:", user);
        console.log("Total Collateral (USD, 8 decimals):", totalCollateral);
        console.log("Total Debt (USD, 8 decimals):", totalDebt);
        console.log("Health Factor (18 decimals):", healthFactor);
        console.log("");
    }

    function deployLiquidatorAtBlock(uint256 blockNumber) internal {
        vm.createSelectFork(
            "https://reth-ethereum.ithaca.xyz/rpc",
            blockNumber - 1
        );

        // Give OWNER some ETH for gas
        vm.deal(OWNER, 100 ether);

        vm.startPrank(OWNER);

        uint256 gasStart = gasleft();
        liquidator = new AaveV3MEVLiquidator();
        uint256 deployGas = gasStart - gasleft();
        console.log("Contract deployment gas:", deployGas);

        liquidator.addWhitelistedDebtAsset(USDC);
        liquidator.addWhitelistedDebtAsset(USDT);
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

        console.log("=== LIQUIDATION TEST ===");
        console.log("Transaction Hash:", vm.toString(liq.txHash));
        console.log("Block:", liq.blockHeight);
        console.log("Transaction Index:", liq.txIndex);
        console.log("User:", liq.user);
        console.log("Debt Asset:", liq.debtAsset);
        console.log("Collateral Asset:", liq.collateralAsset);
        console.log("Debt to cover:", liq.debtToCover);

        AaveV3MEVLiquidator.LiquidationParams[]
            memory params = new AaveV3MEVLiquidator.LiquidationParams[](1);

        params[0] = AaveV3MEVLiquidator.LiquidationParams({
            user: liq.user,
            debtAsset: liq.debtAsset,
            collateralAsset: liq.collateralAsset,
            debtToCover: liq.debtToCover
        });

        // Prepare swap path (single path for all liquidations)
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

        AaveV3MEVLiquidator.SwapPath memory swapPath = AaveV3MEVLiquidator
            .SwapPath({
                pathCollateralToDebt: reverseUniswapPath(pathToDebt),
                pathCollateralToWETH: pathToWeth
            });

        console.log("");
        console.log("=== SWAP PATHS ===");
        console.log(
            "Path Collateral -> Debt length:",
            swapPath.pathCollateralToDebt.length
        );
        console.log(
            "Path Collateral -> WETH length:",
            swapPath.pathCollateralToWETH.length
        );
        console.log("");

        // Check approvals
        console.log("=== APPROVALS ===");
        console.log(
            "WBTC approved to Router:",
            IERC20(liq.collateralAsset).allowance(
                address(liquidator),
                address(0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45)
            )
        );
        console.log(
            "USDT approved to Pool:",
            IERC20(liq.debtAsset).allowance(
                address(liquidator),
                address(0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2)
            )
        );
        console.log("");

        // Replay prior transaction (oracle update) if needed
        if (liq.txIndex > 0) {
            console.log("");
            console.log("=== REPLAYING PRIOR TRANSACTION ===");
            console.log("Block:", liq.blockHeight);
            console.log("Transaction Index:", liq.txIndex - 1);
            replayPriorTransaction(liq.blockHeight, liq.txIndex);
            console.log("");
        }

        console.log("");
        // Log balance before liquidation
        uint256 liquidatorBalanceBefore = address(liquidator).balance;
        uint256 ownerBalanceBefore = OWNER.balance;
        console.log("=== BALANCES BEFORE LIQUIDATION ===");
        console.log("Liquidator Contract:", address(liquidator));
        console.log("  Balance (Wei):", liquidatorBalanceBefore);
        console.log("  Balance (ETH):", liquidatorBalanceBefore / 1e18);
        console.log("");
        console.log("Owner (Profit Recipient):", OWNER);
        console.log("  Balance (Wei):", ownerBalanceBefore);
        console.log("  Balance (ETH):", ownerBalanceBefore / 1e18);
        console.log("");

        // Log Aave position before liquidation
        _logAavePosition(liq.user, "BEFORE");

        uint256 gasStart = gasleft();
        vm.prank(OWNER);
        try
            liquidator.executeLiquidations(params, swapPath, liq.debtToCover, 0)
        {
            uint256 gasUsed = gasStart - gasleft();
            console.log("SUCCESS - Liquidation executed");
            console.log("Gas used for liquidation:", gasUsed);
            console.log("");

            console.log("=== BALANCES AFTER LIQUIDATION ===");
            console.log("Liquidator Contract:", address(liquidator));
            console.log("  ETH Balance (Wei):", address(liquidator).balance);
            console.log(
                "  WETH Balance (Wei):",
                IERC20(WETH).balanceOf(address(liquidator))
            );
            console.log(
                "  Collateral Balance (Wei):",
                IERC20(liq.collateralAsset).balanceOf(address(liquidator))
            );
            console.log(
                "  Debt Asset Balance (Wei):",
                IERC20(liq.debtAsset).balanceOf(address(liquidator))
            );
            console.log("");
            console.log("Owner (Profit Recipient):", OWNER);
            console.log("  ETH Balance (Wei):", OWNER.balance);
            console.log("  WETH Balance (Wei):", IERC20(WETH).balanceOf(OWNER));
            console.log("");

            // Log Aave position after liquidation
            _logAavePosition(liq.user, "AFTER");

            // Log profit/loss summary
            console.log("=== PROFIT/LOSS SUMMARY ===");
            console.log("Liquidator Contract:");
            console.log("  No change (expected - profits sent to owner)");
            console.log("");
            console.log("Owner (Actual Profit):");
            if (OWNER.balance > ownerBalanceBefore) {
                console.log(
                    "  Profit (Wei):",
                    OWNER.balance - ownerBalanceBefore
                );
                console.log(
                    "  Profit (ETH):",
                    (OWNER.balance - ownerBalanceBefore) / 1e18
                );
            } else if (OWNER.balance < ownerBalanceBefore) {
                console.log(
                    "  Loss (Wei):",
                    ownerBalanceBefore - OWNER.balance
                );
                console.log(
                    "  Loss (ETH):",
                    (ownerBalanceBefore - OWNER.balance) / 1e18
                );
            } else {
                console.log("  No profit/loss - Break even");
            }
        } catch Error(string memory reason) {
            console.log("FAILED:", reason);
        } catch (bytes memory lowLevelData) {
            console.log("FAILED: Low-level error");
            console.log("Error data length:", lowLevelData.length);
            if (lowLevelData.length > 0) {
                console.logBytes(lowLevelData);
            }
        }
    }

    /// @notice Replay the transaction that occurred before the liquidation (usually oracle update)
    /// @param blockNumber The block number containing the transaction
    /// @param liquidationTxIndex The index of the liquidation transaction
    function replayPriorTransaction(
        uint256 blockNumber,
        uint256 liquidationTxIndex
    ) internal {
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
            string memory txHash = vm.parseJsonString(txJson, ".hash");

            console.log("Transaction Hash:", txHash);
            console.log("From:", txFrom);
            console.log("To:", txTo);
            console.log("Value (Wei):", txValue);
            console.log("Input Data Length:", bytes(txInput).length, "bytes");

            // Convert string addresses to address type
            address fromAddr = vm.parseAddress(txFrom);
            address toAddr = vm.parseAddress(txTo);
            bytes memory txData = vm.parseBytes(txInput);
            uint256 value = vm.parseUint(txValue);

            // Replay the transaction
            vm.prank(fromAddr);
            (bool success, ) = toAddr.call{value: value}(txData);

            if (success) {
                console.log(
                    "Status: SUCCESS - Transaction replayed successfully"
                );
            } else {
                console.log(
                    "Status: FAILED - Transaction replay failed (may be expected)"
                );
            }
        } catch {
            console.log("Warning: Could not fetch/replay prior transaction");
        }
    }

    function parseUniswapPath(
        string memory pathStr,
        address tokenIn,
        address tokenOut
    ) internal pure returns (bytes memory) {
        if (
            keccak256(bytes(pathStr)) == keccak256(bytes("NO_PATH")) ||
            keccak256(bytes(pathStr)) == keccak256(bytes("N/A")) ||
            bytes(pathStr).length == 0
        ) {
            return "";
        }

        uint24 fee = 3000;
        if (
            (tokenIn == WETH && (tokenOut == USDC || tokenOut == USDT)) ||
            (tokenOut == WETH && (tokenIn == USDC || tokenIn == USDT))
        ) {
            fee = 500;
        }

        return abi.encodePacked(tokenIn, fee, tokenOut);
    }

    function reverseUniswapPath(
        bytes memory path
    ) internal pure returns (bytes memory) {
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

    /// @notice Test liquidation at block 23596063, tx index 2
    /// @dev User: 0x01f8Bd232133fC7502B716b9c1C74762fb20E4bA, Debt: 60084470844, Block: 23596063
    function testLiquidation_Block23596063_TxIndex2() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x01f8Bd232133fC7502B716b9c1C74762fb20E4bA,
            collateralAsset: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599,
            debtAsset: 0xdAC17F958D2ee523a2206206994597C13D831ec7,
            debtToCover: 60084470844,
            liquidatedCollateralAmount: 59602786,
            txHash: 0xf5f78d7fd271094022fdd245ce77a9decde2d44bf3c00a37251143611a6d2095,
            txIndex: 2,
            blockHeight: 23596063,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x9a772018FbD77fcD2d25657e5C547BAfF3Fd7D16",
            pathCollateralToWeth: "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0"
        });

        executeLiquidationTest(liq);
    }
}

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
            blockNumber - 1 // Fork at block before liquidation
        );

        // Give OWNER some ETH for gas
        vm.deal(OWNER, 1 ether);

        vm.startPrank(OWNER);

        uint256 gasStart = gasleft();
        liquidator = new AaveV3MEVLiquidator();
        uint256 deployGas = gasStart - gasleft();
        console.log("Contract deployment gas:", deployGas);

        console.log("Adding whitelisted debt assets...");
        liquidator.addWhitelistedDebtAsset(USDC);
        liquidator.addWhitelistedDebtAsset(USDT);
        liquidator.addWhitelistedDebtAsset(WETH);
        liquidator.addWhitelistedDebtAsset(WBTC);

        console.log("Adding whitelisted collateral assets...");
        liquidator.addWhitelistedCollateralAsset(WETH);
        liquidator.addWhitelistedCollateralAsset(WBTC);
        liquidator.addWhitelistedCollateralAsset(USDC);
        liquidator.addWhitelistedCollateralAsset(USDT);

        console.log("Adding whitelisted liquidator...");
        liquidator.addWhitelistedLiquidator(OWNER);
        vm.stopPrank();

        // Ensure liquidator contract starts with zero balance
        vm.deal(address(liquidator), 0);
        console.log("Liquidator contract balance reset to 0");
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

        console.log("");
        console.log("=== SWAP PATHS ===");
        console.log("Using direct encoding with fee 500");
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
        console.log("=== BALANCES BEFORE LIQUIDATION ===");
        console.log("Liquidator Contract:", address(liquidator));
        console.log("  Balance (Wei):", liquidatorBalanceBefore);
        console.log("  Balance (ETH):", liquidatorBalanceBefore / 1e18);
        console.log("");

        // Log Aave position before liquidation
        _logAavePosition(liq.user, "BEFORE");

        uint256 gasStart = gasleft();
        vm.prank(OWNER);
        try liquidator.executeLiquidations(params, liq.debtToCover, 0) {
            uint256 gasUsed = gasStart - gasleft();
            console.log("SUCCESS - Liquidation executed");
            console.log("Gas used for liquidation:", gasUsed);
            console.log("");

            console.log("=== BALANCES AFTER LIQUIDATION ===");
            console.log("Liquidator Contract:", address(liquidator));
            console.log("  ETH Balance (Wei):", address(liquidator).balance);
            console.log("");

            // Log Aave position after liquidation
            _logAavePosition(liq.user, "AFTER");

            // Log profit/loss summary
            console.log("=== PROFIT/LOSS SUMMARY ===");
            console.log("Liquidator Contract Balance Change:");
            uint256 liquidatorBalanceAfter = address(liquidator).balance;
            if (liquidatorBalanceAfter > liquidatorBalanceBefore) {
                console.log(
                    "  Profit (Wei):",
                    liquidatorBalanceAfter - liquidatorBalanceBefore
                );
            } else if (liquidatorBalanceAfter < liquidatorBalanceBefore) {
                console.log(
                    "  Loss (Wei):",
                    liquidatorBalanceBefore - liquidatorBalanceAfter
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

                // Try to decode common errors
                if (lowLevelData.length >= 68) {
                    bytes4 errorSelector = bytes4(lowLevelData);

                    // InsufficientCollateral(uint256,uint256) = 0xb07e3bc4
                    if (errorSelector == 0xb07e3bc4) {
                        (
                            uint256 collateralReceived,
                            uint256 repaymentNeeded
                        ) = abi.decode(
                                slice(lowLevelData, 4, lowLevelData.length - 4),
                                (uint256, uint256)
                            );
                        console.log("ERROR: InsufficientCollateral");
                        console.log(
                            "  Collateral received:",
                            collateralReceived
                        );
                        console.log("  Repayment needed:", repaymentNeeded);
                    }
                    // SwapFailed(string) = 0x6f670cdb
                    else if (errorSelector == 0x6f670cdb) {
                        string memory swapReason = abi.decode(
                            slice(lowLevelData, 4, lowLevelData.length - 4),
                            (string)
                        );
                        console.log("ERROR: SwapFailed");
                        console.log("  Reason:", swapReason);
                    }
                }
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

        bytes memory result = vm.ffi(inputs);
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
            console.log("Status: SUCCESS - Transaction replayed successfully");
        } else {
            console.log(
                "Status: FAILED - Transaction replay failed (may be expected)"
            );
        }
    }

    receive() external payable {}

    // ========== USDC as Collateral ==========
    // Successful liquidation
    function testLiquidation_USDC_USDC_1() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x1DFFDb114e58baEdBD25290A1EffFa9fC3726D1f,
            collateralAsset: USDC,
            debtAsset: USDC,
            debtToCover: 96737148,
            liquidatedCollateralAmount: 100219685,
            txHash: 0xcb108fe927e00625978d875fe250fbe63ff27f83f6a0127893397f80eb93321c,
            txIndex: 21,
            blockHeight: 23549990,
            originalLiquidator: address(0),
            pathCollateralToDebt: "NO_PATH",
            pathCollateralToWeth: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
        });
        executeLiquidationTest(liq);
    }

    // Successful liquidation
    function testLiquidation_USDC_USDT_1() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x15391E14A74a808f5e7a2055E755BD7f3db97f40,
            collateralAsset: USDC,
            debtAsset: USDT,
            debtToCover: 5905092608,
            liquidatedCollateralAmount: 6130260491,
            txHash: 0x937b67ab76cb96ef95aee73d8f575b54b832d55af3b27523c5cb81b1e7e7dd8d,
            txIndex: 4,
            blockHeight: 23550175,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x9a772018FbD77fcD2d25657e5C547BAfF3Fd7D16",
            pathCollateralToWeth: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
        });
        executeLiquidationTest(liq);
    }

    // Successful liquidation
    function testLiquidation_USDC_WETH_1() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x40Abe5175A7FD0F129A8b24cdB4D2B94E9b39331,
            collateralAsset: USDC,
            debtAsset: WETH,
            debtToCover: 288409361153940476,
            liquidatedCollateralAmount: 1462870118,
            txHash: 0xb29bd1137f559c6aaf49aaaa0e528e3f4128b22fbdda4708e473202c797d717e,
            txIndex: 20,
            blockHeight: 23212509,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
            pathCollateralToWeth: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
        });
        executeLiquidationTest(liq);
    }

    // Successful liquidation
    function testLiquidation_USDC_WBTC_1() public {
        LiquidationData memory liq = LiquidationData({
            user: 0xF259a29046B17C0f3bf04dD9CAF9d8Da91acf96B,
            collateralAsset: USDC,
            debtAsset: WBTC,
            debtToCover: 2086,
            liquidatedCollateralAmount: 2421641,
            txHash: 0x7b26f00a669bec424abed5fdd481eff82da5a200a01579e3ff9014925547a117,
            txIndex: 31,
            blockHeight: 23581422,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x9a772018FbD77fcD2d25657e5C547BAfF3Fd7D16",
            pathCollateralToWeth: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
        });
        executeLiquidationTest(liq);
    }

    // ========== USDT as Collateral ==========
    // Successful liquidation
    function testLiquidation_USDT_USDT_1() public {
        LiquidationData memory liq = LiquidationData({
            user: 0xf674Db418511D722D68438543828a1B4683D11Ec,
            collateralAsset: USDT,
            debtAsset: USDT,
            debtToCover: 6946592,
            liquidatedCollateralAmount: 7227929,
            txHash: 0x1935987f82a80456e51bcffbd7492b0ad3d79bb150a220b46128226c0778e733,
            txIndex: 27,
            blockHeight: 23564710,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x56534741CD8B152df6d48AdF7ac51f75169A83b2",
            pathCollateralToWeth: "0x11b815efB8f581194ae79006d24E0d814B7697F6"
        });
        executeLiquidationTest(liq);
    }

    // Successful liquidation
    function testLiquidation_USDT_USDC_1() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x950E259c88a7A0F5e9AF14c34001323d7F7934F1,
            collateralAsset: USDT,
            debtAsset: USDC,
            debtToCover: 75121649886,
            liquidatedCollateralAmount: 78127956881,
            txHash: 0x6bdb153b2745aa47bd7377a0f98cc481edbf7b3005109a8b64f1814dac920e16,
            txIndex: 22,
            blockHeight: 23549982,
            originalLiquidator: address(0),
            pathCollateralToDebt: "NO_PATH",
            pathCollateralToWeth: "0x11b815efB8f581194ae79006d24E0d814B7697F6"
        });
        executeLiquidationTest(liq);
    }

    // Successful liquidation
    function testLiquidation_USDT_WETH_1() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x62ba65C5e10eAaB63a7D195523a12ba7E57609f1,
            collateralAsset: USDT,
            debtAsset: WETH,
            debtToCover: 4828914066305727,
            liquidatedCollateralAmount: 20522449,
            txHash: 0x0ce9a54dcda3a1a057ee0e859e7802068148fd17b8514fac6cb40dd9dd60bd54,
            txIndex: 95,
            blockHeight: 23101167,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x11b815efB8f581194ae79006d24E0d814B7697F6",
            pathCollateralToWeth: "0x11b815efB8f581194ae79006d24E0d814B7697F6"
        });
        executeLiquidationTest(liq);
    }

    // Successful liquidation
    function testLiquidation_USDT_WBTC_1() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x0B97e4aDa36F37B7cD077Af015898DAE7f6103A1,
            collateralAsset: USDT,
            debtAsset: WBTC,
            debtToCover: 7601,
            liquidatedCollateralAmount: 9323980,
            txHash: 0xc8f3ab13cfbf78ec1bf3d89cb101c3eb2b380298ea3738597286b65ba79ceb03,
            txIndex: 14,
            blockHeight: 23163994,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x56534741CD8B152df6d48AdF7ac51f75169A83b2",
            pathCollateralToWeth: "0x11b815efB8f581194ae79006d24E0d814B7697F6"
        });
        executeLiquidationTest(liq);
    }

    // ========== WETH as Collateral ==========
    // Successful liquidation
    function testLiquidation_WETH_USDC_1() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x9bA742eAf83004d2e650B087f4B6c456323521e5,
            collateralAsset: WETH,
            debtAsset: USDC,
            debtToCover: 1007636781,
            liquidatedCollateralAmount: 271854234289796801,
            txHash: 0x1cb5829b8a07c6818775d4e72b64747755863c15f72e583c335a2a26751216f1,
            txIndex: 12,
            blockHeight: 23623487,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
            pathCollateralToWeth: "N/A"
        });
        executeLiquidationTest(liq);
    }

    // Successful liquidation
    function testLiquidation_WETH_USDT_1() public {
        LiquidationData memory liq = LiquidationData({
            user: 0xe6e0d89FeAcff96eC7A53b09021C68286A592676,
            collateralAsset: WETH,
            debtAsset: USDT,
            debtToCover: 304001265,
            liquidatedCollateralAmount: 78694639765785253,
            txHash: 0x0734cf02849786fa2f9964c8396f8c3eb0fa620c0affe45afce64e07b2040f42,
            txIndex: 15,
            blockHeight: 23617604,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x11b815efB8f581194ae79006d24E0d814B7697F6",
            pathCollateralToWeth: "N/A"
        });
        executeLiquidationTest(liq);
    }

    // Successful liquidation
    function testLiquidation_WETH_WETH_1() public {
        LiquidationData memory liq = LiquidationData({
            user: 0xcc07Ad7063FbAAAB12391ADB3199f9fe6A66Fc6C,
            collateralAsset: WETH,
            debtAsset: WETH,
            debtToCover: 38310323841570308,
            liquidatedCollateralAmount: 38655116756144441,
            txHash: 0xa5b8b17caf5a37be99f7756aa7e77a6a77da66ad79d6e4fa24d861d9e1edea53,
            txIndex: 8,
            blockHeight: 23598345,
            originalLiquidator: address(0),
            pathCollateralToDebt: "NO_PATH",
            pathCollateralToWeth: "N/A"
        });
        executeLiquidationTest(liq);
    }

    // Successful liquidation
    function testLiquidation_WETH_WBTC_1() public {
        LiquidationData memory liq = LiquidationData({
            user: 0xeAdDCDAb0Bb43a2d7324cA2bB3f8098Fd2e937c1,
            collateralAsset: WETH,
            debtAsset: WBTC,
            debtToCover: 6177,
            liquidatedCollateralAmount: 1909764885367559,
            txHash: 0xe86d63bfef393606e15843186dd424d62e56b8445b7e2f5111b5f003879501c3,
            txIndex: 60,
            blockHeight: 23558685,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0",
            pathCollateralToWeth: "N/A"
        });
        executeLiquidationTest(liq);
    }

    // ========== WBTC as Collateral ==========
    // Successful liquidation
    function testLiquidation_WBTC_USDC_1() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x473561C22C71C062037Fc6D38d50e8CAFae346dd,
            collateralAsset: WBTC,
            debtAsset: USDC,
            debtToCover: 128411968,
            liquidatedCollateralAmount: 129457,
            txHash: 0xe9d52872d7b0b65110d92b58f30ec755be130f4f0c62eab099359718ad6f6657,
            txIndex: 129,
            blockHeight: 23596765,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x9a772018FbD77fcD2d25657e5C547BAfF3Fd7D16",
            pathCollateralToWeth: "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0"
        });
        executeLiquidationTest(liq);
    }

    // Successful liquidation
    function testLiquidation_WBTC_USDT_1() public {
        LiquidationData memory liq = LiquidationData({
            user: 0xad63886E494639bE9Bb0FcE9A9E49a8dE7a6133f,
            collateralAsset: WBTC,
            debtAsset: USDT,
            debtToCover: 7336268758,
            liquidatedCollateralAmount: 7397945,
            txHash: 0x4887a73a68ba1cc6a32f8db2586cc8a5eba8c4d29f78cc4ecc0649793b43f192,
            txIndex: 1,
            blockHeight: 23596764,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x56534741CD8B152df6d48AdF7ac51f75169A83b2",
            pathCollateralToWeth: "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0"
        });
        executeLiquidationTest(liq);
    }

    // Successful liquidation
    function testLiquidation_WBTC_WETH_1() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x820d2924FC880A3C045e64d35390a1FaEB4F431f,
            collateralAsset: WBTC,
            debtAsset: WETH,
            debtToCover: 253567738275020008,
            liquidatedCollateralAmount: 983039,
            txHash: 0x0e1047209aee4e894949b8761abd32494d5b472f2d377a16e203c073f3fe345e,
            txIndex: 51,
            blockHeight: 23106463,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0",
            pathCollateralToWeth: "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0"
        });
        executeLiquidationTest(liq);
    }

    // Successful liquidation
    function testLiquidation_WBTC_WBTC_1() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x316016ed07031cea56D615230c354cFa14975E15,
            collateralAsset: WBTC,
            debtAsset: WBTC,
            debtToCover: 402949,
            liquidatedCollateralAmount: 421081,
            txHash: 0x28fe5a0c25205e27a1ea987cb4d2511e11d8b2d59b9d91df98c37d7612aa4cf4,
            txIndex: 54,
            blockHeight: 23416680,
            originalLiquidator: address(0),
            pathCollateralToDebt: "NO_PATH",
            pathCollateralToWeth: "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0"
        });
        executeLiquidationTest(liq);
    }

    /// @notice Helper function to slice bytes
    function slice(
        bytes memory data,
        uint256 start,
        uint256 length
    ) internal pure returns (bytes memory) {
        bytes memory result = new bytes(length);
        for (uint256 i = 0; i < length; i++) {
            result[i] = data[start + i];
        }
        return result;
    }
}

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

    /// @notice Test liquidation #198 at block 23549979, tx index 1
    /// @dev User: 0x0Ed0eA598e89765471e86A060bd91F4BB2dc1fB0, Debt: 2160343471, Block: 23549979
    function testLiquidation_0198_Block23549979() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x0Ed0eA598e89765471e86A060bd91F4BB2dc1fB0,
            collateralAsset: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599,
            debtAsset: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599,
            debtToCover: 2160343471,
            liquidatedCollateralAmount: 2257558927,
            txHash: 0xeb04da1e80f3db8edde16a1ffb7827123c1dcfeb76ca0e605ad20724b708fed8,
            txIndex: 1,
            blockHeight: 23549979,
            originalLiquidator: address(0),
            pathCollateralToDebt: "NO_PATH",
            pathCollateralToWeth: "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #528 at block 23416680, tx index 54
    /// @dev User: 0x316016ed07031cea56D615230c354cFa14975E15, Debt: 402949, Block: 23416680
    function testLiquidation_0528_Block23416680() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x316016ed07031cea56D615230c354cFa14975E15,
            collateralAsset: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599,
            debtAsset: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599,
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


    /// @notice Test liquidation #84 at block 23581422, tx index 31
    /// @dev User: 0xF259a29046B17C0f3bf04dD9CAF9d8Da91acf96B, Debt: 2086, Block: 23581422
    function testLiquidation_0084_Block23581422() public {
        LiquidationData memory liq = LiquidationData({
            user: 0xF259a29046B17C0f3bf04dD9CAF9d8Da91acf96B,
            collateralAsset: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
            debtAsset: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599,
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


    /// @notice Test liquidation #210 at block 23549976, tx index 5
    /// @dev User: 0x9bafc93D0A8Cef41Eef7fE74b157DcE04E948363, Debt: 18969598, Block: 23549976
    function testLiquidation_0210_Block23549976() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x9bafc93D0A8Cef41Eef7fE74b157DcE04E948363,
            collateralAsset: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
            debtAsset: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599,
            debtToCover: 18969598,
            liquidatedCollateralAmount: 20934113797,
            txHash: 0xde8828bc6e502e522c0c9afb1b09ef3d276f0b68f531bef4d65e65f98465b256,
            txIndex: 5,
            blockHeight: 23549976,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x9a772018FbD77fcD2d25657e5C547BAfF3Fd7D16",
            pathCollateralToWeth: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #90 at block 23558685, tx index 60
    /// @dev User: 0xeAdDCDAb0Bb43a2d7324cA2bB3f8098Fd2e937c1, Debt: 6177, Block: 23558685
    function testLiquidation_0090_Block23558685() public {
        LiquidationData memory liq = LiquidationData({
            user: 0xeAdDCDAb0Bb43a2d7324cA2bB3f8098Fd2e937c1,
            collateralAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtAsset: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599,
            debtToCover: 6177,
            liquidatedCollateralAmount: 1909764885367559,
            txHash: 0xe86d63bfef393606e15843186dd424d62e56b8445b7e2f5111b5f003879501c3,
            txIndex: 60,
            blockHeight: 23558685,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0",
            pathCollateralToWeth: "NO_PATH"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #171 at block 23549989, tx index 405
    /// @dev User: 0x4AcAf39A1a9b42440e57890905D6655dCBb5ED59, Debt: 6574529, Block: 23549989
    function testLiquidation_0171_Block23549989() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x4AcAf39A1a9b42440e57890905D6655dCBb5ED59,
            collateralAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtAsset: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599,
            debtToCover: 6574529,
            liquidatedCollateralAmount: 2116727387042406549,
            txHash: 0x21a7e0261792ca6bc0ccd3435530d6d9b30f642937b96bcc462e032d0dd0cb73,
            txIndex: 405,
            blockHeight: 23549989,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0",
            pathCollateralToWeth: "NO_PATH"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #2 at block 23626689, tx index 1
    /// @dev User: 0x2337191Bec1Ae25bd43bC65579F1122E5942e25B, Debt: 15002678, Block: 23626689
    function testLiquidation_0002_Block23626689() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x2337191Bec1Ae25bd43bC65579F1122E5942e25B,
            collateralAsset: 0xdAC17F958D2ee523a2206206994597C13D831ec7,
            debtAsset: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599,
            debtToCover: 15002678,
            liquidatedCollateralAmount: 17524767339,
            txHash: 0xcfe797044c29054eb4e459a05880adf9c6d9fb92b89c3d1e629943f5b48ad808,
            txIndex: 1,
            blockHeight: 23626689,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x56534741CD8B152df6d48AdF7ac51f75169A83b2",
            pathCollateralToWeth: "0x11b815efB8f581194ae79006d24E0d814B7697F6"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #614 at block 23310541, tx index 57
    /// @dev User: 0x931C339c1d6A8058A54c490952cB1336b1A9A42c, Debt: 3439, Block: 23310541
    function testLiquidation_0614_Block23310541() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x931C339c1d6A8058A54c490952cB1336b1A9A42c,
            collateralAsset: 0xdAC17F958D2ee523a2206206994597C13D831ec7,
            debtAsset: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599,
            debtToCover: 3439,
            liquidatedCollateralAmount: 3980903,
            txHash: 0xe2b17c235dfa07bbb36379531addcbe51a944d130b26fad2c8926dc991a3328a,
            txIndex: 57,
            blockHeight: 23310541,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x56534741CD8B152df6d48AdF7ac51f75169A83b2",
            pathCollateralToWeth: "0x11b815efB8f581194ae79006d24E0d814B7697F6"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #8 at block 23598712, tx index 34
    /// @dev User: 0x0D04E320E18aabC6D8c9563783a9A0CeC4ECac89, Debt: 32004234, Block: 23598712
    function testLiquidation_0008_Block23598712() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x0D04E320E18aabC6D8c9563783a9A0CeC4ECac89,
            collateralAsset: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599,
            debtAsset: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
            debtToCover: 32004234,
            liquidatedCollateralAmount: 31380,
            txHash: 0xfba44f918a3c4f06064edf4fcf464357aeb3564ff96fd96246f2c97c5d9c0e98,
            txIndex: 34,
            blockHeight: 23598712,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x9a772018FbD77fcD2d25657e5C547BAfF3Fd7D16",
            pathCollateralToWeth: "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #11 at block 23596765, tx index 129
    /// @dev User: 0x473561C22C71C062037Fc6D38d50e8CAFae346dd, Debt: 128411968, Block: 23596765
    function testLiquidation_0011_Block23596765() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x473561C22C71C062037Fc6D38d50e8CAFae346dd,
            collateralAsset: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599,
            debtAsset: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
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


    /// @notice Test liquidation #170 at block 23549990, tx index 21
    /// @dev User: 0x1DFFDb114e58baEdBD25290A1EffFa9fC3726D1f, Debt: 96737148, Block: 23549990
    function testLiquidation_0170_Block23549990() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x1DFFDb114e58baEdBD25290A1EffFa9fC3726D1f,
            collateralAsset: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
            debtAsset: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
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


    /// @notice Test liquidation #188 at block 23549983, tx index 5
    /// @dev User: 0x8DD0284B7E6DfA1F7aAabd39e21243b707dE65d2, Debt: 449624920, Block: 23549983
    function testLiquidation_0188_Block23549983() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x8DD0284B7E6DfA1F7aAabd39e21243b707dE65d2,
            collateralAsset: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
            debtAsset: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
            debtToCover: 449624920,
            liquidatedCollateralAmount: 465811417,
            txHash: 0xe590ff96f5b9920110c59381afa4834be6302b9c8257b6b137f47d69d2aebeac,
            txIndex: 5,
            blockHeight: 23549983,
            originalLiquidator: address(0),
            pathCollateralToDebt: "NO_PATH",
            pathCollateralToWeth: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #1 at block 23628573, tx index 3
    /// @dev User: 0x51493Edf5f67D1e061b2D8D7cAFbF085B1A844E0, Debt: 38383822409, Block: 23628573
    function testLiquidation_0001_Block23628573() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x51493Edf5f67D1e061b2D8D7cAFbF085B1A844E0,
            collateralAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtAsset: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
            debtToCover: 38383822409,
            liquidatedCollateralAmount: 10205685487720712948,
            txHash: 0x33d9202a4810eaa58411d7d3104a76972ffc25a0985d3f2fb3fa278e5f679324,
            txIndex: 3,
            blockHeight: 23628573,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
            pathCollateralToWeth: "NO_PATH"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #3 at block 23623487, tx index 12
    /// @dev User: 0x9bA742eAf83004d2e650B087f4B6c456323521e5, Debt: 1007636781, Block: 23623487
    function testLiquidation_0003_Block23623487() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x9bA742eAf83004d2e650B087f4B6c456323521e5,
            collateralAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtAsset: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
            debtToCover: 1007636781,
            liquidatedCollateralAmount: 271854234289796801,
            txHash: 0x1cb5829b8a07c6818775d4e72b64747755863c15f72e583c335a2a26751216f1,
            txIndex: 12,
            blockHeight: 23623487,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
            pathCollateralToWeth: "NO_PATH"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #191 at block 23549982, tx index 22
    /// @dev User: 0x950E259c88a7A0F5e9AF14c34001323d7F7934F1, Debt: 75121649886, Block: 23549982
    function testLiquidation_0191_Block23549982() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x950E259c88a7A0F5e9AF14c34001323d7F7934F1,
            collateralAsset: 0xdAC17F958D2ee523a2206206994597C13D831ec7,
            debtAsset: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
            debtToCover: 75121649886,
            liquidatedCollateralAmount: 78127956881,
            txHash: 0x6bdb153b2745aa47bd7377a0f98cc481edbf7b3005109a8b64f1814dac920e16,
            txIndex: 22,
            blockHeight: 23549982,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x7858E59e0C01EA06Df3aF3D20aC7B0003275D4Bf",
            pathCollateralToWeth: "0x11b815efB8f581194ae79006d24E0d814B7697F6"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #428 at block 23450476, tx index 74
    /// @dev User: 0xD0ED626826D0F4e58692B17b7202Fef2eBE2660A, Debt: 1066300484303445, Block: 23450476
    function testLiquidation_0428_Block23450476() public {
        LiquidationData memory liq = LiquidationData({
            user: 0xD0ED626826D0F4e58692B17b7202Fef2eBE2660A,
            collateralAsset: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599,
            debtAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtToCover: 1066300484303445,
            liquidatedCollateralAmount: 4083,
            txHash: 0x3461c5bd45d33520ba379360675c9ce2c10cee39107da463c85cadabd0605086,
            txIndex: 74,
            blockHeight: 23450476,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0",
            pathCollateralToWeth: "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #689 at block 23212519, tx index 5
    /// @dev User: 0x784E182EF06522096877C49f55b0a8EB9bd3e013, Debt: 1183019600250111183, Block: 23212519
    function testLiquidation_0689_Block23212519() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x784E182EF06522096877C49f55b0a8EB9bd3e013,
            collateralAsset: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599,
            debtAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtToCover: 1183019600250111183,
            liquidatedCollateralAmount: 5320208,
            txHash: 0x61b445654303e7084817714db59ad3ef6fbb077febade48b4ca16c292f963cf2,
            txIndex: 5,
            blockHeight: 23212519,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0",
            pathCollateralToWeth: "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #128 at block 23551590, tx index 82
    /// @dev User: 0xb2633E23645FCF4857567C90E96f82e1c7Ba6725, Debt: 98529454616740618, Block: 23551590
    function testLiquidation_0128_Block23551590() public {
        LiquidationData memory liq = LiquidationData({
            user: 0xb2633E23645FCF4857567C90E96f82e1c7Ba6725,
            collateralAsset: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
            debtAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtToCover: 98529454616740618,
            liquidatedCollateralAmount: 392919291,
            txHash: 0x291fcedcebe664582ed64335747abb19ee2e32f1d91c0a7b0ce3302ff516195f,
            txIndex: 82,
            blockHeight: 23551590,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
            pathCollateralToWeth: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #690 at block 23212519, tx index 4
    /// @dev User: 0x84887512c5C34Bf478D4B963d0aEC18D7Cd590B5, Debt: 3918230417920962956, Block: 23212519
    function testLiquidation_0690_Block23212519() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x84887512c5C34Bf478D4B963d0aEC18D7Cd590B5,
            collateralAsset: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
            debtAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtToCover: 3918230417920962956,
            liquidatedCollateralAmount: 19983567774,
            txHash: 0x32691e905d059d4043909c58840cfb5484d60a7f61582186184e98d99bc280e0,
            txIndex: 4,
            blockHeight: 23212519,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
            pathCollateralToWeth: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #9 at block 23598345, tx index 8
    /// @dev User: 0xcc07Ad7063FbAAAB12391ADB3199f9fe6A66Fc6C, Debt: 38310323841570308, Block: 23598345
    function testLiquidation_0009_Block23598345() public {
        LiquidationData memory liq = LiquidationData({
            user: 0xcc07Ad7063FbAAAB12391ADB3199f9fe6A66Fc6C,
            collateralAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtToCover: 38310323841570308,
            liquidatedCollateralAmount: 38655116756144441,
            txHash: 0xa5b8b17caf5a37be99f7756aa7e77a6a77da66ad79d6e4fa24d861d9e1edea53,
            txIndex: 8,
            blockHeight: 23598345,
            originalLiquidator: address(0),
            pathCollateralToDebt: "NO_PATH",
            pathCollateralToWeth: "NO_PATH"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #75 at block 23589242, tx index 22
    /// @dev User: 0xCbB74E8eAbCD36B160D1fC3BEd7bc6E52D327632, Debt: 980291907942409, Block: 23589242
    function testLiquidation_0075_Block23589242() public {
        LiquidationData memory liq = LiquidationData({
            user: 0xCbB74E8eAbCD36B160D1fC3BEd7bc6E52D327632,
            collateralAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtToCover: 980291907942409,
            liquidatedCollateralAmount: 1024405043799817,
            txHash: 0x25c438fd40d335b535230a466391f4c62700997e3c98b935641a6869b379d8d6,
            txIndex: 22,
            blockHeight: 23589242,
            originalLiquidator: address(0),
            pathCollateralToDebt: "NO_PATH",
            pathCollateralToWeth: "NO_PATH"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #516 at block 23428944, tx index 150
    /// @dev User: 0xEF17dE13a3DFD889f5259e5f1E4e97C5cdf0E602, Debt: 1162847590285113, Block: 23428944
    function testLiquidation_0516_Block23428944() public {
        LiquidationData memory liq = LiquidationData({
            user: 0xEF17dE13a3DFD889f5259e5f1E4e97C5cdf0E602,
            collateralAsset: 0xdAC17F958D2ee523a2206206994597C13D831ec7,
            debtAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtToCover: 1162847590285113,
            liquidatedCollateralAmount: 5057318,
            txHash: 0x277caecddd102a156025a32774abfaa74aa876ea7b9bf9a7a345d90d2ee8c0aa,
            txIndex: 150,
            blockHeight: 23428944,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x11b815efB8f581194ae79006d24E0d814B7697F6",
            pathCollateralToWeth: "0x11b815efB8f581194ae79006d24E0d814B7697F6"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #655 at block 23221136, tx index 1
    /// @dev User: 0x0923496E43E9feB6e548DD65E581E4dc90b4c82B, Debt: 1000024769829221888, Block: 23221136
    function testLiquidation_0655_Block23221136() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x0923496E43E9feB6e548DD65E581E4dc90b4c82B,
            collateralAsset: 0xdAC17F958D2ee523a2206206994597C13D831ec7,
            debtAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtToCover: 1000024769829221888,
            liquidatedCollateralAmount: 4577077490,
            txHash: 0x95d5b42d5baca0ba63f6e0e140a0ef840a1ac576718558da7c69228e31c9e34d,
            txIndex: 1,
            blockHeight: 23221136,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x11b815efB8f581194ae79006d24E0d814B7697F6",
            pathCollateralToWeth: "0x11b815efB8f581194ae79006d24E0d814B7697F6"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #12 at block 23596764, tx index 14
    /// @dev User: 0x0eA439da1522f2eefeEab963d1c06aFE4B7d6E96, Debt: 6800815975, Block: 23596764
    function testLiquidation_0012_Block23596764() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x0eA439da1522f2eefeEab963d1c06aFE4B7d6E96,
            collateralAsset: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599,
            debtAsset: 0xdAC17F958D2ee523a2206206994597C13D831ec7,
            debtToCover: 6800815975,
            liquidatedCollateralAmount: 6857991,
            txHash: 0x6e0e08965fa531b95f0c346d4ec19f1ce2b450aedf8f83174dce2810ceb111fd,
            txIndex: 14,
            blockHeight: 23596764,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x56534741CD8B152df6d48AdF7ac51f75169A83b2",
            pathCollateralToWeth: "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #13 at block 23596764, tx index 13
    /// @dev User: 0xad63886E494639bE9Bb0FcE9A9E49a8dE7a6133f, Debt: 7336268758, Block: 23596764
    function testLiquidation_0013_Block23596764() public {
        LiquidationData memory liq = LiquidationData({
            user: 0xad63886E494639bE9Bb0FcE9A9E49a8dE7a6133f,
            collateralAsset: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599,
            debtAsset: 0xdAC17F958D2ee523a2206206994597C13D831ec7,
            debtToCover: 7336268758,
            liquidatedCollateralAmount: 7397945,
            txHash: 0x4887a73a68ba1cc6a32f8db2586cc8a5eba8c4d29f78cc4ecc0649793b43f192,
            txIndex: 13,
            blockHeight: 23596764,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x56534741CD8B152df6d48AdF7ac51f75169A83b2",
            pathCollateralToWeth: "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #143 at block 23550879, tx index 45
    /// @dev User: 0x15391E14A74a808f5e7a2055E755BD7f3db97f40, Debt: 990925466, Block: 23550879
    function testLiquidation_0143_Block23550879() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x15391E14A74a808f5e7a2055E755BD7f3db97f40,
            collateralAsset: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
            debtAsset: 0xdAC17F958D2ee523a2206206994597C13D831ec7,
            debtToCover: 990925466,
            liquidatedCollateralAmount: 1028717459,
            txHash: 0x9b3ca43c657b65755db90798d8fc2c0b665dba0adfd5363d8dda9a0f40a8a78f,
            txIndex: 45,
            blockHeight: 23550879,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x7858E59e0C01EA06Df3aF3D20aC7B0003275D4Bf",
            pathCollateralToWeth: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #155 at block 23550175, tx index 4
    /// @dev User: 0x15391E14A74a808f5e7a2055E755BD7f3db97f40, Debt: 5905092608, Block: 23550175
    function testLiquidation_0155_Block23550175() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x15391E14A74a808f5e7a2055E755BD7f3db97f40,
            collateralAsset: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
            debtAsset: 0xdAC17F958D2ee523a2206206994597C13D831ec7,
            debtToCover: 5905092608,
            liquidatedCollateralAmount: 6130260491,
            txHash: 0x937b67ab76cb96ef95aee73d8f575b54b832d55af3b27523c5cb81b1e7e7dd8d,
            txIndex: 4,
            blockHeight: 23550175,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x7858E59e0C01EA06Df3aF3D20aC7B0003275D4Bf",
            pathCollateralToWeth: "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #5 at block 23619867, tx index 75
    /// @dev User: 0xfbf2D20Ddb7D7e17EBb2b2F1Dfa9402ba10296a1, Debt: 1130194580, Block: 23619867
    function testLiquidation_0005_Block23619867() public {
        LiquidationData memory liq = LiquidationData({
            user: 0xfbf2D20Ddb7D7e17EBb2b2F1Dfa9402ba10296a1,
            collateralAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtAsset: 0xdAC17F958D2ee523a2206206994597C13D831ec7,
            debtToCover: 1130194580,
            liquidatedCollateralAmount: 295969274733668984,
            txHash: 0xc22023ec92c41c1f8e37f3480bfee8007eca8a31acae7c9c4928f662d873426b,
            txIndex: 75,
            blockHeight: 23619867,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x11b815efB8f581194ae79006d24E0d814B7697F6",
            pathCollateralToWeth: "NO_PATH"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #6 at block 23617604, tx index 15
    /// @dev User: 0xe6e0d89FeAcff96eC7A53b09021C68286A592676, Debt: 304001265, Block: 23617604
    function testLiquidation_0006_Block23617604() public {
        LiquidationData memory liq = LiquidationData({
            user: 0xe6e0d89FeAcff96eC7A53b09021C68286A592676,
            collateralAsset: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
            debtAsset: 0xdAC17F958D2ee523a2206206994597C13D831ec7,
            debtToCover: 304001265,
            liquidatedCollateralAmount: 78694639765785253,
            txHash: 0x0734cf02849786fa2f9964c8396f8c3eb0fa620c0affe45afce64e07b2040f42,
            txIndex: 15,
            blockHeight: 23617604,
            originalLiquidator: address(0),
            pathCollateralToDebt: "0x11b815efB8f581194ae79006d24E0d814B7697F6",
            pathCollateralToWeth: "NO_PATH"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #88 at block 23564710, tx index 27
    /// @dev User: 0xf674Db418511D722D68438543828a1B4683D11Ec, Debt: 6946592, Block: 23564710
    function testLiquidation_0088_Block23564710() public {
        LiquidationData memory liq = LiquidationData({
            user: 0xf674Db418511D722D68438543828a1B4683D11Ec,
            collateralAsset: 0xdAC17F958D2ee523a2206206994597C13D831ec7,
            debtAsset: 0xdAC17F958D2ee523a2206206994597C13D831ec7,
            debtToCover: 6946592,
            liquidatedCollateralAmount: 7227929,
            txHash: 0x1935987f82a80456e51bcffbd7492b0ad3d79bb150a220b46128226c0778e733,
            txIndex: 27,
            blockHeight: 23564710,
            originalLiquidator: address(0),
            pathCollateralToDebt: "NO_PATH",
            pathCollateralToWeth: "0x11b815efB8f581194ae79006d24E0d814B7697F6"
        });

        executeLiquidationTest(liq);
    }


    /// @notice Test liquidation #162 at block 23549992, tx index 2
    /// @dev User: 0x15391E14A74a808f5e7a2055E755BD7f3db97f40, Debt: 2856644232, Block: 23549992
    function testLiquidation_0162_Block23549992() public {
        LiquidationData memory liq = LiquidationData({
            user: 0x15391E14A74a808f5e7a2055E755BD7f3db97f40,
            collateralAsset: 0xdAC17F958D2ee523a2206206994597C13D831ec7,
            debtAsset: 0xdAC17F958D2ee523a2206206994597C13D831ec7,
            debtToCover: 2856644232,
            liquidatedCollateralAmount: 2972338323,
            txHash: 0x0626fd58b4b49669b454ec3a0b1dcba013801736da701715bad09bc00a3e77ab,
            txIndex: 2,
            blockHeight: 23549992,
            originalLiquidator: address(0),
            pathCollateralToDebt: "NO_PATH",
            pathCollateralToWeth: "0x11b815efB8f581194ae79006d24E0d814B7697F6"
        });

        executeLiquidationTest(liq);
    }

}

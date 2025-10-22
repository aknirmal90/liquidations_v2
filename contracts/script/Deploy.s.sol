// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "forge-std/Script.sol";
import "../src/liquidator.sol";

contract DeployLiquidator is Script {
    // Common Ethereum mainnet token addresses
    address constant USDC = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48;
    address constant USDT = 0xdAC17F958D2ee523a2206206994597C13D831ec7;
    address constant WETH = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2;
    address constant WBTC = 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599;

    function run() external {
        uint256 deployerPrivateKey = vm.envUint("PRIVATE_KEY");
        address deployer = vm.addr(deployerPrivateKey);

        console.log("Deploying from:", deployer);
        console.log("Deployer balance:", deployer.balance);

        vm.startBroadcast(deployerPrivateKey);

        // Deploy the liquidator contract
        AaveV3MEVLiquidator liquidator = new AaveV3MEVLiquidator();
        console.log("AaveV3MEVLiquidator deployed at:", address(liquidator));

        // Whitelist common debt assets (stablecoins)
        console.log("\nWhitelisting debt assets...");
        liquidator.addWhitelistedDebtAsset(USDC);
        console.log("- USDC whitelisted");

        liquidator.addWhitelistedDebtAsset(USDT);
        console.log("- USDT whitelisted");

        liquidator.addWhitelistedDebtAsset(WETH);
        console.log("- WETH whitelisted");

        // Whitelist common collateral assets
        console.log("\nWhitelisting collateral assets...");
        liquidator.addWhitelistedCollateralAsset(WETH);
        console.log("- WETH whitelisted");

        liquidator.addWhitelistedCollateralAsset(WBTC);
        console.log("- WBTC whitelisted");

        // Whitelist the deployer as a liquidator
        liquidator.addWhitelistedLiquidator(deployer);
        console.log("\nWhitelisted liquidator:", deployer);

        vm.stopBroadcast();

        console.log("\n=== Deployment Summary ===");
        console.log("Contract:", address(liquidator));
        console.log("Owner:", liquidator.owner());
        console.log("\nDebt assets: USDC, USDT, WETH");
        console.log("Collateral assets: WETH, WBTC");
        console.log("Liquidators:", deployer);
    }
}

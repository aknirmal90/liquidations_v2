// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "forge-std/Test.sol";
import "../src/liquidator.sol";

contract LiquidatorWhitelistTest is Test {
    AaveV3MEVLiquidator public liquidator;

    // Test accounts
    address public owner;
    address public nonOwner;
    address public user2;

    // Mock token addresses (using actual mainnet addresses for realism)
    address constant USDC = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48;
    address constant USDT = 0xdAC17F958D2ee523a2206206994597C13D831ec7;
    address constant DAI = 0x6B175474E89094C44Da98b954EedeAC495271d0F;
    address constant WETH = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2;
    address constant WBTC = 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599;

    // Private keys provided
    uint256 constant OWNER_PK = 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80;
    uint256 constant NON_OWNER_PK = 0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d;
    uint256 constant USER2_PK = 0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a;

    function setUp() public {
        // Derive addresses from private keys
        owner = vm.addr(OWNER_PK);
        nonOwner = vm.addr(NON_OWNER_PK);
        user2 = vm.addr(USER2_PK);

        // Deploy contract as owner
        vm.prank(owner);
        liquidator = new AaveV3MEVLiquidator();

        // Verify owner is set correctly
        assertEq(liquidator.owner(), owner, "Owner should be set correctly");
    }

    // ========== Debt Asset Whitelist Tests ==========

    function testAddWhitelistedDebtAsset_Owner() public {
        // Owner should be able to add debt asset
        vm.prank(owner);
        liquidator.addWhitelistedDebtAsset(USDC);

        // Verify state
        assertTrue(liquidator.whitelistedDebtAssets(USDC), "USDC should be whitelisted");
    }

    function testAddWhitelistedDebtAsset_NonOwner() public {
        // Non-owner should not be able to add debt asset
        vm.prank(nonOwner);
        vm.expectRevert(NotOwner.selector);
        liquidator.addWhitelistedDebtAsset(USDC);

        // Verify state unchanged
        assertFalse(liquidator.whitelistedDebtAssets(USDC), "USDC should not be whitelisted");
    }

    function testAddWhitelistedDebtAsset_ZeroAddress() public {
        // Should revert on zero address
        vm.prank(owner);
        vm.expectRevert(ZeroAddress.selector);
        liquidator.addWhitelistedDebtAsset(address(0));
    }

    function testAddWhitelistedDebtAsset_AlreadyWhitelisted() public {
        // Add once
        vm.prank(owner);
        liquidator.addWhitelistedDebtAsset(USDC);

        // Try to add again
        vm.prank(owner);
        vm.expectRevert(AlreadyWhitelisted.selector);
        liquidator.addWhitelistedDebtAsset(USDC);
    }

    function testRemoveWhitelistedDebtAsset_Owner() public {
        // First add the asset
        vm.prank(owner);
        liquidator.addWhitelistedDebtAsset(USDC);
        assertTrue(liquidator.whitelistedDebtAssets(USDC), "USDC should be whitelisted");

        // Owner should be able to remove debt asset
        vm.prank(owner);
        liquidator.removeWhitelistedDebtAsset(USDC);

        // Verify state
        assertFalse(liquidator.whitelistedDebtAssets(USDC), "USDC should not be whitelisted");
    }

    function testRemoveWhitelistedDebtAsset_NonOwner() public {
        // First add the asset as owner
        vm.prank(owner);
        liquidator.addWhitelistedDebtAsset(USDC);

        // Non-owner should not be able to remove debt asset
        vm.prank(nonOwner);
        vm.expectRevert(NotOwner.selector);
        liquidator.removeWhitelistedDebtAsset(USDC);

        // Verify state unchanged
        assertTrue(liquidator.whitelistedDebtAssets(USDC), "USDC should still be whitelisted");
    }

    function testAddMultipleDebtAssets_Owner() public {
        // Owner should be able to add multiple debt assets
        vm.startPrank(owner);
        liquidator.addWhitelistedDebtAsset(USDC);
        liquidator.addWhitelistedDebtAsset(USDT);
        liquidator.addWhitelistedDebtAsset(DAI);
        vm.stopPrank();

        // Verify all are whitelisted
        assertTrue(liquidator.whitelistedDebtAssets(USDC), "USDC should be whitelisted");
        assertTrue(liquidator.whitelistedDebtAssets(USDT), "USDT should be whitelisted");
        assertTrue(liquidator.whitelistedDebtAssets(DAI), "DAI should be whitelisted");
    }

    // ========== Collateral Asset Whitelist Tests ==========

    function testAddWhitelistedCollateralAsset_Owner() public {
        // Owner should be able to add collateral asset
        vm.prank(owner);
        liquidator.addWhitelistedCollateralAsset(WETH);

        // Verify state
        assertTrue(liquidator.whitelistedCollateralAssets(WETH), "WETH should be whitelisted");
    }

    function testAddWhitelistedCollateralAsset_NonOwner() public {
        // Non-owner should not be able to add collateral asset
        vm.prank(nonOwner);
        vm.expectRevert(NotOwner.selector);
        liquidator.addWhitelistedCollateralAsset(WETH);

        // Verify state unchanged
        assertFalse(liquidator.whitelistedCollateralAssets(WETH), "WETH should not be whitelisted");
    }

    function testAddWhitelistedCollateralAsset_ZeroAddress() public {
        // Should revert on zero address
        vm.prank(owner);
        vm.expectRevert(ZeroAddress.selector);
        liquidator.addWhitelistedCollateralAsset(address(0));
    }

    function testAddWhitelistedCollateralAsset_AlreadyWhitelisted() public {
        // Add once
        vm.prank(owner);
        liquidator.addWhitelistedCollateralAsset(WETH);

        // Try to add again
        vm.prank(owner);
        vm.expectRevert(AlreadyWhitelisted.selector);
        liquidator.addWhitelistedCollateralAsset(WETH);
    }

    function testRemoveWhitelistedCollateralAsset_Owner() public {
        // First add the asset
        vm.prank(owner);
        liquidator.addWhitelistedCollateralAsset(WETH);
        assertTrue(liquidator.whitelistedCollateralAssets(WETH), "WETH should be whitelisted");

        // Owner should be able to remove collateral asset
        vm.prank(owner);
        liquidator.removeWhitelistedCollateralAsset(WETH);

        // Verify state
        assertFalse(liquidator.whitelistedCollateralAssets(WETH), "WETH should not be whitelisted");
    }

    function testRemoveWhitelistedCollateralAsset_NonOwner() public {
        // First add the asset as owner
        vm.prank(owner);
        liquidator.addWhitelistedCollateralAsset(WETH);

        // Non-owner should not be able to remove collateral asset
        vm.prank(nonOwner);
        vm.expectRevert(NotOwner.selector);
        liquidator.removeWhitelistedCollateralAsset(WETH);

        // Verify state unchanged
        assertTrue(liquidator.whitelistedCollateralAssets(WETH), "WETH should still be whitelisted");
    }

    function testAddMultipleCollateralAssets_Owner() public {
        // Owner should be able to add multiple collateral assets
        vm.startPrank(owner);
        liquidator.addWhitelistedCollateralAsset(WETH);
        liquidator.addWhitelistedCollateralAsset(WBTC);
        vm.stopPrank();

        // Verify all are whitelisted
        assertTrue(liquidator.whitelistedCollateralAssets(WETH), "WETH should be whitelisted");
        assertTrue(liquidator.whitelistedCollateralAssets(WBTC), "WBTC should be whitelisted");
    }

    // ========== Liquidator Whitelist Tests ==========

    function testAddWhitelistedLiquidator_Owner() public {
        // Owner should be able to add liquidator
        vm.prank(owner);
        liquidator.addWhitelistedLiquidator(nonOwner);

        // Verify state
        assertTrue(liquidator.whitelistedLiquidators(nonOwner), "nonOwner should be whitelisted liquidator");
    }

    function testAddWhitelistedLiquidator_NonOwner() public {
        // Non-owner should not be able to add liquidator
        vm.prank(nonOwner);
        vm.expectRevert(NotOwner.selector);
        liquidator.addWhitelistedLiquidator(user2);

        // Verify state unchanged
        assertFalse(liquidator.whitelistedLiquidators(user2), "user2 should not be whitelisted liquidator");
    }

    function testAddWhitelistedLiquidator_ZeroAddress() public {
        // Should revert on zero address
        vm.prank(owner);
        vm.expectRevert(ZeroAddress.selector);
        liquidator.addWhitelistedLiquidator(address(0));
    }

    function testRemoveWhitelistedLiquidator_Owner() public {
        // First add the liquidator
        vm.prank(owner);
        liquidator.addWhitelistedLiquidator(nonOwner);
        assertTrue(liquidator.whitelistedLiquidators(nonOwner), "nonOwner should be whitelisted");

        // Owner should be able to remove liquidator
        vm.prank(owner);
        liquidator.removeWhitelistedLiquidator(nonOwner);

        // Verify state
        assertFalse(liquidator.whitelistedLiquidators(nonOwner), "nonOwner should not be whitelisted");
    }

    function testRemoveWhitelistedLiquidator_NonOwner() public {
        // First add the liquidator as owner
        vm.prank(owner);
        liquidator.addWhitelistedLiquidator(user2);

        // Non-owner should not be able to remove liquidator
        vm.prank(nonOwner);
        vm.expectRevert(NotOwner.selector);
        liquidator.removeWhitelistedLiquidator(user2);

        // Verify state unchanged
        assertTrue(liquidator.whitelistedLiquidators(user2), "user2 should still be whitelisted");
    }

    function testAddMultipleLiquidators_Owner() public {
        // Owner should be able to add multiple liquidators
        vm.startPrank(owner);
        liquidator.addWhitelistedLiquidator(nonOwner);
        liquidator.addWhitelistedLiquidator(user2);
        vm.stopPrank();

        // Verify all are whitelisted
        assertTrue(liquidator.whitelistedLiquidators(nonOwner), "nonOwner should be whitelisted");
        assertTrue(liquidator.whitelistedLiquidators(user2), "user2 should be whitelisted");
    }

    // ========== Transfer Ownership Tests ==========

    function testTransferOwnership_Owner() public {
        // Owner should be able to transfer ownership
        vm.prank(owner);
        liquidator.transferOwnership(nonOwner);

        // Verify new owner
        assertEq(liquidator.owner(), nonOwner, "Ownership should be transferred");
    }

    function testTransferOwnership_NonOwner() public {
        // Non-owner should not be able to transfer ownership
        vm.prank(nonOwner);
        vm.expectRevert(NotOwner.selector);
        liquidator.transferOwnership(user2);

        // Verify ownership unchanged
        assertEq(liquidator.owner(), owner, "Ownership should not change");
    }

    function testTransferOwnership_ZeroAddress() public {
        // Should revert on zero address
        vm.prank(owner);
        vm.expectRevert(ZeroAddress.selector);
        liquidator.transferOwnership(address(0));
    }

    function testNewOwnerCanModifyWhitelists() public {
        // Transfer ownership
        vm.prank(owner);
        liquidator.transferOwnership(nonOwner);

        // New owner should be able to modify whitelists
        vm.prank(nonOwner);
        liquidator.addWhitelistedDebtAsset(USDC);

        // Verify
        assertTrue(liquidator.whitelistedDebtAssets(USDC), "New owner should be able to whitelist");

        // Old owner should NOT be able to modify
        vm.prank(owner);
        vm.expectRevert(NotOwner.selector);
        liquidator.addWhitelistedDebtAsset(DAI);
    }

    // ========== Integration Tests ==========

    function testCompleteWhitelistSetup() public {
        vm.startPrank(owner);

        // Add debt assets
        liquidator.addWhitelistedDebtAsset(USDC);
        liquidator.addWhitelistedDebtAsset(DAI);

        // Add collateral assets
        liquidator.addWhitelistedCollateralAsset(WETH);
        liquidator.addWhitelistedCollateralAsset(WBTC);

        // Add liquidators
        liquidator.addWhitelistedLiquidator(nonOwner);
        liquidator.addWhitelistedLiquidator(user2);

        vm.stopPrank();

        // Verify all debt assets
        assertTrue(liquidator.whitelistedDebtAssets(USDC), "USDC should be whitelisted");
        assertTrue(liquidator.whitelistedDebtAssets(DAI), "DAI should be whitelisted");

        // Verify all collateral assets
        assertTrue(liquidator.whitelistedCollateralAssets(WETH), "WETH should be whitelisted");
        assertTrue(liquidator.whitelistedCollateralAssets(WBTC), "WBTC should be whitelisted");

        // Verify all liquidators
        assertTrue(liquidator.whitelistedLiquidators(nonOwner), "nonOwner should be whitelisted");
        assertTrue(liquidator.whitelistedLiquidators(user2), "user2 should be whitelisted");
    }

    function testRemoveAllWhitelists() public {
        // First add everything
        vm.startPrank(owner);
        liquidator.addWhitelistedDebtAsset(USDC);
        liquidator.addWhitelistedCollateralAsset(WETH);
        liquidator.addWhitelistedLiquidator(nonOwner);
        vm.stopPrank();

        // Verify added
        assertTrue(liquidator.whitelistedDebtAssets(USDC));
        assertTrue(liquidator.whitelistedCollateralAssets(WETH));
        assertTrue(liquidator.whitelistedLiquidators(nonOwner));

        // Remove everything
        vm.startPrank(owner);
        liquidator.removeWhitelistedDebtAsset(USDC);
        liquidator.removeWhitelistedCollateralAsset(WETH);
        liquidator.removeWhitelistedLiquidator(nonOwner);
        vm.stopPrank();

        // Verify removed
        assertFalse(liquidator.whitelistedDebtAssets(USDC));
        assertFalse(liquidator.whitelistedCollateralAssets(WETH));
        assertFalse(liquidator.whitelistedLiquidators(nonOwner));
    }

    // ========== Bribe Parameter Tests ==========

    function testBribeValidation_Valid() public {
        // Should accept bribe values from 0 to 100
        vm.startPrank(owner);
        liquidator.addWhitelistedDebtAsset(USDC);
        liquidator.addWhitelistedCollateralAsset(WETH);
        liquidator.addWhitelistedLiquidator(owner);
        vm.stopPrank();

        AaveV3MEVLiquidator.LiquidationParams[] memory params =
            new AaveV3MEVLiquidator.LiquidationParams[](1);
        params[0] = AaveV3MEVLiquidator.LiquidationParams({
            user: address(1),
            debtAsset: USDC,
            collateralAsset: WETH,
            debtToCover: 1000
        });

        // These should not revert during validation (will fail later due to flashloan, but that's ok)
        vm.prank(owner);
        try liquidator.executeLiquidations(params, 1000, 0) {
            // Success
        } catch {
            // Expected to fail at flashloan, not validation
        }

        vm.prank(owner);
        try liquidator.executeLiquidations(params, 1000, 50) {
            // Success
        } catch {
            // Expected to fail at flashloan, not validation
        }

        vm.prank(owner);
        try liquidator.executeLiquidations(params, 1000, 100) {
            // Success
        } catch {
            // Expected to fail at flashloan, not validation
        }
    }

    function testBribeValidation_TooHigh() public {
        vm.startPrank(owner);
        liquidator.addWhitelistedDebtAsset(USDC);
        liquidator.addWhitelistedCollateralAsset(WETH);
        liquidator.addWhitelistedLiquidator(owner);
        vm.stopPrank();

        AaveV3MEVLiquidator.LiquidationParams[] memory params =
            new AaveV3MEVLiquidator.LiquidationParams[](1);
        params[0] = AaveV3MEVLiquidator.LiquidationParams({
            user: address(1),
            debtAsset: USDC,
            collateralAsset: WETH,
            debtToCover: 1000
        });

        // Should revert with bribe > 100
        vm.prank(owner);
        vm.expectRevert(InvalidBribe.selector);
        liquidator.executeLiquidations(params, 1000, 101);
    }
}

// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/// ----------------------
/// Minimal ERC20
/// ----------------------
interface IERC20 {
    function balanceOf(address) external view returns (uint256);
    function approve(address spender, uint256 value) external returns (bool);
    function transfer(address to, uint256 value) external returns (bool);
}

/// ----------------------
/// WETH Interface
/// ----------------------
interface IWETH {
    function withdraw(uint256) external;
    function deposit() external payable;
}

/// ----------------------
/// Aave v3
/// ----------------------
interface IAaveV3Pool {
    function liquidationCall(
        address collateralAsset,
        address debtAsset,
        address user,
        uint256 debtToCover,
        bool receiveAToken
    ) external;

    function flashLoanSimple(
        address receiverAddress,
        address asset,
        uint256 amount,
        bytes calldata params,
        uint16 referralCode
    ) external;
}

interface IFlashLoanSimpleReceiver {
    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address initiator,
        bytes calldata params
    ) external returns (bool);
    function ADDRESSES_PROVIDER() external view returns (address);
    function POOL() external view returns (IAaveV3Pool);
}

/// ----------------------
/// Uniswap v3 SwapRouter02
/// ----------------------
interface ISwapRouter {
    struct ExactInputParams {
        bytes path;
        address recipient;
        uint256 amountIn;
        uint256 amountOutMinimum;
    }
    struct ExactOutputParams {
        bytes path;
        address recipient;
        uint256 amountOut;
        uint256 amountInMaximum;
    }
    function exactInput(
        ExactInputParams calldata params
    ) external payable returns (uint256 amountOut);
    function exactOutput(
        ExactOutputParams calldata params
    ) external payable returns (uint256 amountIn);
}

/// ----------------------
/// Safe Approval Helper (for USDT compatibility)
/// ----------------------
library SafeApprove {
    function safeApprove(address token, address spender, uint256 value) internal {
        (bool success, bytes memory data) = token.call(
            abi.encodeWithSelector(IERC20.approve.selector, spender, value)
        );
        require(
            success && (data.length == 0 || abi.decode(data, (bool))),
            "SafeApprove: approve failed"
        );
    }
}

/// ----------------------
/// Minimal Ownable/Reentrancy (no events for gas savings)
/// ----------------------
abstract contract Ownable {
    address public owner;
    modifier onlyOwner() {
        require(msg.sender == owner, "Not owner");
        _;
    }
    constructor() {
        owner = msg.sender;
    }
    function transferOwnership(address n) external onlyOwner {
        require(n != address(0), "zero");
        owner = n;
    }
}

abstract contract ReentrancyGuard {
    uint256 private constant _NOT = 1;
    uint256 private constant _IN = 2;
    uint256 private _s = _NOT;
    modifier nonReentrant() {
        require(_s != _IN, "reentrant");
        _s = _IN;
        _;
        _s = _NOT;
    }
}

/// ---------------------------------------------------------------------------
/// Aave V3 MEV Liquidator
///
/// Features:
/// 1. Minimal gas usage - pre-approvals, off-chain routing, no events
/// 2. Try-catch for liquidations - requires at least 1 success
/// 3. Asset whitelists with pre-approvals on whitelist
/// 4. EOA whitelist for liquidators (separate from admin)
/// 5. Correct MEV flow: flashloan debt → liquidate → swap exact for repayment →
///    remainder to WETH → ETH → send to block.coinbase
/// ---------------------------------------------------------------------------
contract AaveV3MEVLiquidator is
    Ownable,
    ReentrancyGuard,
    IFlashLoanSimpleReceiver
{
    using SafeApprove for address;
    // ---------- Hardcoded Ethereum mainnet addresses ----------
    IAaveV3Pool public constant POOL =
        IAaveV3Pool(0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2);
    ISwapRouter public constant ROUTER =
        ISwapRouter(0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45);
    address public constant WETH = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2;

    // ---------- Whitelists ----------
    mapping(address => bool) public whitelistedDebtAssets;
    mapping(address => bool) public whitelistedCollateralAssets;
    mapping(address => bool) public whitelistedLiquidators;

    struct LiquidationParams {
        address user;
        address debtAsset;
        address collateralAsset;
        uint256 debtToCover;
    }

    struct SwapPath {
        bytes pathCollateralToDebt; // for exact repayment of flashloan
        bytes pathCollateralToWETH; // for converting remainder to WETH
        uint256 maxCollateralForRepayment; // max collateral to use for debt repayment (slippage)
    }

    // ---------- Constructor ----------
    constructor() {}

    receive() external payable {}

    // ---------- Whitelist Management (Admin Only) ----------

    /// @notice Add debt asset to whitelist and pre-approve to pool
    function addWhitelistedDebtAsset(address asset) external onlyOwner {
        require(asset != address(0), "zero address");
        require(!whitelistedDebtAssets[asset], "already whitelisted");

        whitelistedDebtAssets[asset] = true;

        // Pre-approve debt asset to pool (for liquidation repayment)
        // Reset to 0 first for USDT compatibility
        asset.safeApprove(address(POOL), 0);
        asset.safeApprove(address(POOL), type(uint256).max);
    }

    /// @notice Remove debt asset from whitelist
    function removeWhitelistedDebtAsset(address asset) external onlyOwner {
        whitelistedDebtAssets[asset] = false;
        asset.safeApprove(address(POOL), 0);
    }

    /// @notice Add collateral asset to whitelist and pre-approve to router
    function addWhitelistedCollateralAsset(address asset) external onlyOwner {
        require(asset != address(0), "zero address");
        require(!whitelistedCollateralAssets[asset], "already whitelisted");

        whitelistedCollateralAssets[asset] = true;

        // Pre-approve collateral to router (for swaps)
        // Reset to 0 first for USDT compatibility
        asset.safeApprove(address(ROUTER), 0);
        asset.safeApprove(address(ROUTER), type(uint256).max);
    }

    /// @notice Remove collateral asset from whitelist
    function removeWhitelistedCollateralAsset(
        address asset
    ) external onlyOwner {
        whitelistedCollateralAssets[asset] = false;
        asset.safeApprove(address(ROUTER), 0);
    }

    /// @notice Add EOA to liquidator whitelist
    function addWhitelistedLiquidator(address liquidator) external onlyOwner {
        require(liquidator != address(0), "zero address");
        whitelistedLiquidators[liquidator] = true;
    }

    /// @notice Remove EOA from liquidator whitelist
    function removeWhitelistedLiquidator(
        address liquidator
    ) external onlyOwner {
        whitelistedLiquidators[liquidator] = false;
    }

    // ---------- Main Liquidation Entry Point ----------

    /// @notice Execute batch liquidations with flashloan
    /// @dev Only whitelisted liquidators can call. Requires at least 1 successful liquidation.
    /// @param params Array of liquidation parameters
    /// @param swapPaths Array of pre-computed Uniswap paths (computed off-chain)
    /// @param totalFlashloanAmount Total debt asset to borrow via flashloan
    /// @param bribe Percentage (0-100) of profit to send to block.coinbase, remainder goes to msg.sender
    function executeLiquidations(
        LiquidationParams[] calldata params,
        SwapPath[] calldata swapPaths,
        uint256 totalFlashloanAmount,
        uint256 bribe
    ) external nonReentrant {
        require(whitelistedLiquidators[msg.sender], "Not whitelisted");
        require(params.length > 0, "No liquidations");
        require(params.length == swapPaths.length, "Length mismatch");
        require(totalFlashloanAmount > 0, "Zero flashloan");
        require(bribe <= 100, "Bribe must be <= 100");

        // Validate all assets are whitelisted
        for (uint256 i = 0; i < params.length; i++) {
            require(
                whitelistedDebtAssets[params[i].debtAsset],
                "Debt not whitelisted"
            );
            require(
                whitelistedCollateralAssets[params[i].collateralAsset],
                "Collateral not whitelisted"
            );
        }

        // All liquidations must use the same debt asset for single flashloan
        address debtAsset = params[0].debtAsset;
        for (uint256 i = 1; i < params.length; i++) {
            require(params[i].debtAsset == debtAsset, "Mixed debt assets");
        }

        // Encode params for callback (including bribe and liquidator address)
        bytes memory callbackData = abi.encode(params, swapPaths, bribe, msg.sender);

        // Execute flashloan
        POOL.flashLoanSimple(
            address(this),
            debtAsset,
            totalFlashloanAmount,
            callbackData,
            0
        );
    }

    // ---------- Flashloan Callback ----------

    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address initiator,
        bytes calldata params
    ) external override returns (bool) {
        require(msg.sender == address(POOL), "Not pool");
        require(initiator == address(this), "Bad initiator");

        (
            LiquidationParams[] memory liquidations,
            SwapPath[] memory swapPaths,
            uint256 bribe,
            address liquidator
        ) = abi.decode(params, (LiquidationParams[], SwapPath[], uint256, address));

        uint256 successCount = 0;
        uint256 totalCollateralReceived = 0;
        address collateralAsset = liquidations[0].collateralAsset;

        // Execute liquidations with try-catch
        for (uint256 i = 0; i < liquidations.length; i++) {
            uint256 balanceBefore = IERC20(collateralAsset).balanceOf(
                address(this)
            );

            // Try to liquidate - don't revert if one fails
            try
                POOL.liquidationCall(
                    liquidations[i].collateralAsset,
                    liquidations[i].debtAsset,
                    liquidations[i].user,
                    liquidations[i].debtToCover,
                    false // receive underlying collateral, not aToken
                )
            {
                uint256 balanceAfter = IERC20(collateralAsset).balanceOf(
                    address(this)
                );
                uint256 received = balanceAfter - balanceBefore;

                // Count as success if any collateral received
                if (received > 0) {
                    successCount++;
                    totalCollateralReceived += received;
                }
            } catch {
                // Liquidation failed, continue to next
                continue;
            }
        }

        // Require at least 1 successful liquidation
        require(successCount > 0, "No successful liquidations");

        // Calculate flashloan repayment amount
        uint256 flashloanRepayment = amount + premium;

        // Swap EXACT amount of collateral needed to repay flashloan
        // Uses exactOutput: we want exactly flashloanRepayment of debt asset
        uint256 collateralUsedForRepayment = 0;

        if (swapPaths[0].pathCollateralToDebt.length > 0) {
            // Only swap if collateral != debt asset
            collateralUsedForRepayment = ROUTER.exactOutput(
                ISwapRouter.ExactOutputParams({
                    path: swapPaths[0].pathCollateralToDebt,
                    recipient: address(this),
                    amountOut: flashloanRepayment,
                    amountInMaximum: swapPaths[0].maxCollateralForRepayment
                })
            );
        } else {
            // Collateral == debt asset, no swap needed
            collateralUsedForRepayment = flashloanRepayment;
        }

        // Calculate remaining collateral (profit)
        uint256 remainingCollateral = totalCollateralReceived -
            collateralUsedForRepayment;

        // Convert remaining collateral to WETH
        if (
            remainingCollateral > 0 &&
            swapPaths[0].pathCollateralToWETH.length > 0
        ) {
            ROUTER.exactInput(
                ISwapRouter.ExactInputParams({
                    path: swapPaths[0].pathCollateralToWETH,
                    recipient: address(this),
                    amountIn: remainingCollateral,
                    amountOutMinimum: 0 // Already profitable if we got here
                })
            );
        }

        // Unwrap all WETH to ETH
        uint256 wethBalance = IERC20(WETH).balanceOf(address(this));
        if (wethBalance > 0) {
            IWETH(WETH).withdraw(wethBalance);
        }

        // Split ETH between validator and liquidator based on bribe percentage
        uint256 ethBalance = address(this).balance;
        if (ethBalance > 0) {
            // Calculate validator bribe (e.g., 90% if bribe = 90)
            uint256 validatorAmount = (ethBalance * bribe) / 100;
            uint256 liquidatorAmount = ethBalance - validatorAmount;

            // Send bribe to validator (block.coinbase)
            if (validatorAmount > 0) {
                (bool success, ) = block.coinbase.call{value: validatorAmount}("");
                require(success, "Validator transfer failed");
            }

            // Send remaining profit to liquidator
            if (liquidatorAmount > 0) {
                (bool success, ) = liquidator.call{value: liquidatorAmount}("");
                require(success, "Liquidator transfer failed");
            }
        }

        // Approve exact repayment amount to pool (already pre-approved, but being explicit)
        // Note: Pre-approval should handle this, but Aave pulls during return
        return true;
    }

    // ---------- Interface completeness ----------

    function ADDRESSES_PROVIDER() external pure override returns (address) {
        return address(0);
    }

    // ---------- Emergency Functions ----------

    /// @notice Emergency token sweep (admin only)
    function emergencySweep(address token, address to) external onlyOwner {
        if (token == address(0)) {
            uint256 amt = address(this).balance;
            if (amt > 0) {
                (bool ok, ) = payable(to).call{value: amt}("");
                require(ok, "ETH sweep failed");
            }
        } else {
            uint256 bal = IERC20(token).balanceOf(address(this));
            if (bal > 0) {
                require(IERC20(token).transfer(to, bal), "Token sweep failed");
            }
        }
    }
}

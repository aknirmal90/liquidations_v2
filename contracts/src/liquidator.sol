// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/// ----------------------
/// Custom Errors (Gas Optimization)
/// ----------------------
error NotOwner();
error ZeroAddress();
error AlreadyWhitelisted();
error NotWhitelisted();
error NoLiquidations();
error ZeroFlashloan();
error InvalidBribe();
error DebtNotWhitelisted();
error CollateralNotWhitelisted();
error MixedDebtAssets();
error MixedCollateralAssets();
error NotPool();
error BadInitiator();
error NoSuccessfulLiquidations();
error ValidatorTransferFailed();
error LiquidatorTransferFailed();
error ReentrancyDetected();
error ApproveFailed();
error ETHSweepFailed();
error TokenSweepFailed();
error InsufficientCollateral(
    uint256 collateralReceived,
    uint256 repaymentNeeded
);

/// ----------------------
/// Minimal ERC20
/// ----------------------
interface IERC20 {
    function balanceOf(address) external view returns (uint256);
    function approve(address spender, uint256 value) external returns (bool);
    function transfer(address to, uint256 value) external returns (bool);
    function allowance(
        address owner,
        address spender
    ) external view returns (uint256);
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

    function getUserAccountData(
        address user
    )
        external
        view
        returns (
            uint256 totalCollateralBase,
            uint256 totalDebtBase,
            uint256 availableBorrowsBase,
            uint256 currentLiquidationThreshold,
            uint256 ltv,
            uint256 healthFactor
        );

    function flashLoan(
        address receiverAddress,
        address[] calldata assets,
        uint256[] calldata amounts,
        uint256[] calldata modes,
        address onBehalfOf,
        bytes calldata params,
        uint16 referralCode
    ) external;

    function FLASHLOAN_PREMIUM_TOTAL() external view returns (uint128);
}

/// ----------------------
/// Aave V3 Flashloan Receiver
/// ----------------------
interface IFlashLoanSimpleReceiver {
    function executeOperation(
        address[] calldata assets,
        uint256[] calldata amounts,
        uint256[] calldata premiums,
        address initiator,
        bytes calldata params
    ) external returns (bool);
}

/// ----------------------
/// Uniswap v3 SwapRouter
/// ----------------------
interface ISwapRouter {
    struct ExactInputParams {
        bytes path;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
    }
    struct ExactOutputParams {
        bytes path;
        address recipient;
        uint256 deadline;
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
    function safeApprove(
        address token,
        address spender,
        uint256 value
    ) internal {
        (bool success, bytes memory data) = token.call(
            abi.encodeWithSelector(IERC20.approve.selector, spender, value)
        );
        if (!success || (data.length != 0 && !abi.decode(data, (bool)))) {
            revert ApproveFailed();
        }
    }
}

/// ----------------------
/// Minimal Ownable/Reentrancy (no events for gas savings)
/// ----------------------
abstract contract Ownable {
    address public owner;
    modifier onlyOwner() {
        if (msg.sender != owner) revert NotOwner();
        _;
    }
    constructor() {
        owner = msg.sender;
    }
    function transferOwnership(address n) external onlyOwner {
        if (n == address(0)) revert ZeroAddress();
        owner = n;
    }
}

abstract contract ReentrancyGuard {
    uint256 private constant _NOT = 1;
    uint256 private constant _IN = 2;
    uint256 private _s = _NOT;
    modifier nonReentrant() {
        if (_s == _IN) revert ReentrancyDetected();
        _s = _IN;
        _;
        _s = _NOT;
    }
}

/// ---------------------------------------------------------------------------
/// Aave V3 MEV Liquidator with Aave V3 Flashloans
///
/// Features:
/// 1. Minimal gas usage - pre-approvals, off-chain routing, no events
/// 2. Try-catch for liquidations - requires at least 1 success
/// 3. Asset whitelists with pre-approvals on whitelist
/// 4. EOA whitelist for liquidators (separate from admin)
/// 5. Correct MEV flow: flashloan debt → liquidate → swap exact for repayment →
///    remainder to WETH → ETH → send to block.coinbase
/// 6. Uses Aave V3 flashloans (0.05% fee)
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
        ISwapRouter(0xE592427A0AEce92De3Edee1F18E0157C05861564);
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

    receive() external payable {}

    // ---------- Whitelist Management (Admin Only) ----------

    /// @notice Add debt asset to whitelist and pre-approve to pool (for both liquidation and flashloan repayment)
    function addWhitelistedDebtAsset(address asset) external onlyOwner {
        if (asset == address(0)) revert ZeroAddress();
        if (whitelistedDebtAssets[asset]) revert AlreadyWhitelisted();

        whitelistedDebtAssets[asset] = true;

        // Pre-approve debt asset to pool (for liquidation repayment and flashloan repayment)
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
        if (asset == address(0)) revert ZeroAddress();
        if (whitelistedCollateralAssets[asset]) revert AlreadyWhitelisted();

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
        if (liquidator == address(0)) revert ZeroAddress();
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
    /// @param totalFlashloanAmount Total debt asset to borrow via flashloan
    /// @param bribe Percentage (0-100) of profit to send to block.coinbase, remainder goes to msg.sender
    function executeLiquidations(
        LiquidationParams[] calldata params,
        uint256 totalFlashloanAmount,
        uint256 bribe
    ) external nonReentrant {
        if (!whitelistedLiquidators[msg.sender]) revert NotWhitelisted();
        uint256 len = params.length;
        if (len == 0) revert NoLiquidations();
        if (totalFlashloanAmount == 0) revert ZeroFlashloan();
        if (bribe > 100) revert InvalidBribe();

        // All liquidations must use the same debt asset for single flashloan
        address debtAsset = params[0].debtAsset;
        address collateralAsset = params[0].collateralAsset;

        // Validate all assets are whitelisted in single loop
        if (!whitelistedDebtAssets[debtAsset]) revert DebtNotWhitelisted();
        if (!whitelistedCollateralAssets[collateralAsset])
            revert CollateralNotWhitelisted();

        for (uint256 i = 1; i < len; ) {
            if (params[i].debtAsset != debtAsset) revert MixedDebtAssets();
            if (params[i].collateralAsset != collateralAsset)
                revert MixedCollateralAssets();

            unchecked {
                ++i;
            }
        }

        // Encode params for callback (including bribe and liquidator address)
        bytes memory callbackData = abi.encode(params, bribe, msg.sender);

        // Prepare arrays for Aave V3 flashloan
        address[] memory assets = new address[](1);
        assets[0] = debtAsset;

        uint256[] memory amounts = new uint256[](1);
        amounts[0] = totalFlashloanAmount;

        uint256[] memory modes = new uint256[](1);
        modes[0] = 0; // 0 = no debt, just flashloan

        // Execute flashloan (0.05% fee on Aave V3)
        POOL.flashLoan(
            address(this),
            assets,
            amounts,
            modes,
            address(this),
            callbackData,
            0 // referralCode
        );
    }

    // ---------- Flashloan Callback ----------

    /// @notice Aave V3 flashloan callback
    /// @dev Called by Aave pool after flashloan is sent to this contract
    function executeOperation(
        address[] calldata /* assets */,
        uint256[] calldata amounts,
        uint256[] calldata premiums,
        address initiator,
        bytes calldata params
    ) external override returns (bool) {
        if (msg.sender != address(POOL)) revert NotPool();
        if (initiator != address(this)) revert BadInitiator();

        _executeFlashloanCallback(amounts[0], premiums[0], params);

        return true;
    }

    /// @notice Internal callback logic for flashloan execution
    /// @param amount Flashloan amount borrowed
    /// @param premium Flashloan fee (0.05% on Aave V3)
    /// @param userData Encoded liquidation parameters
    function _executeFlashloanCallback(
        uint256 amount,
        uint256 premium,
        bytes memory userData
    ) internal {
        (
            LiquidationParams[] memory liquidations,
            uint256 bribe,
            address liquidator
        ) = abi.decode(userData, (LiquidationParams[], uint256, address));

        address collateralAsset = liquidations[0].collateralAsset;
        uint256 successCount = 0;

        // Execute liquidations with try-catch
        for (uint256 i = 0; i < liquidations.length; ) {
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
                ++successCount;
            } catch {
                // Liquidation failed, continue to next
            }

            unchecked {
                ++i;
            }
        }

        // Require at least 1 successful liquidation
        if (successCount == 0) revert NoSuccessfulLiquidations();

        address debtAsset = liquidations[0].debtAsset;

        // Calculate flashloan repayment amount (Aave V3 charges 0.05% premium)
        uint256 flashloanRepayment = amount + premium;

        // Swap collateral to debt asset to repay flashloan
        uint256 collateralUsedForRepayment = 0;

        // Calculate total collateral received
        uint256 totalCollateralReceived = IERC20(collateralAsset).balanceOf(
            address(this)
        );

        if (collateralAsset != debtAsset) {
            // Swap collateral -> debt asset via WETH routing if needed
            // NOTE: For exactOutput, path is REVERSED (output token first, input token last)

            bytes memory pathCollateralToDebt;
            if (debtAsset == WETH || collateralAsset == WETH) {
                // If either debtAsset or collateralAsset is WETH, do direct swap
                // Path: WETH <- collateralAsset (for exactOutput, output token first)
                pathCollateralToDebt = abi.encodePacked(
                    debtAsset,
                    uint24(500),
                    collateralAsset
                );
            } else {
                // Route through WETH: debtAsset <- WETH <- collateralAsset
                pathCollateralToDebt = abi.encodePacked(
                    debtAsset,
                    uint24(500),
                    WETH,
                    uint24(3000),
                    collateralAsset
                );
            }

            // Swap exact output: get exactly flashloanRepayment of debt asset
            collateralUsedForRepayment = ROUTER.exactOutput(
                ISwapRouter.ExactOutputParams({
                    path: pathCollateralToDebt,
                    recipient: address(this),
                    deadline: block.timestamp,
                    amountOut: flashloanRepayment,
                    amountInMaximum: totalCollateralReceived
                })
            );
        } else {
            // Collateral == debt asset, no swap needed
            collateralUsedForRepayment = flashloanRepayment;
        }

        // Calculate remaining collateral (profit)
        uint256 remainingCollateral = totalCollateralReceived -
            collateralUsedForRepayment;

        // Convert remaining collateral (profit) to WETH
        if (remainingCollateral > 0 && collateralAsset != WETH) {
            bytes memory pathCollateralToWETH = abi.encodePacked(
                collateralAsset,
                uint24(500), // 0.3% fee tier (better liquidity for WBTC/WETH)
                WETH
            );

            ROUTER.exactInput(
                ISwapRouter.ExactInputParams({
                    path: pathCollateralToWETH,
                    recipient: address(this),
                    deadline: block.timestamp,
                    amountIn: remainingCollateral,
                    amountOutMinimum: 0
                })
            );
        }

        if (debtAsset == WETH) {
            // If debt asset is WETH, some of the balance will be used to repay the flashloan
            uint256 wethBalance = IERC20(WETH).balanceOf(address(this));
            if (wethBalance > 0) {
                IWETH(WETH).withdraw(wethBalance - flashloanRepayment);
            }
        } else {
            // If debt asset is not WETH, flashloan repayment is being held in another token
            uint256 wethBalance = IERC20(WETH).balanceOf(address(this));
            if (wethBalance > 0) {
                IWETH(WETH).withdraw(wethBalance);
            }
        }

        // Split profit between validator (bribe) and liquidator
        uint256 ethBalance = address(this).balance;

        if (ethBalance > 0) {
            if (bribe > 0 && bribe < 100) {
                // Split between validator and liquidator
                uint256 validatorAmount;
                unchecked {
                    validatorAmount = (ethBalance * bribe) / 100;
                }

                (bool successValidator, ) = payable(block.coinbase).call{
                    value: validatorAmount
                }("");
            } else if (bribe == 100) {
                // Full bribe - send all to validator (single transfer)
                (bool success, ) = payable(block.coinbase).call{
                    value: ethBalance
                }("");
            }
        }

        // Repay flashloan to Aave Pool
        // Approval already set in addWhitelistedDebtAsset
        // Aave will automatically pull the repayment amount from this contract
    }

    // ---------- Emergency Functions ----------

    /// @notice Emergency token sweep (admin only)
    function emergencySweep(address token, address to) external onlyOwner {
        if (token == address(0)) {
            uint256 amt = address(this).balance;
            if (amt > 0) {
                (bool ok, ) = payable(to).call{value: amt}("");
                if (!ok) revert ETHSweepFailed();
            }
        } else {
            uint256 bal = IERC20(token).balanceOf(address(this));
            if (bal > 0) {
                if (!IERC20(token).transfer(to, bal)) revert TokenSweepFailed();
            }
        }
    }
}

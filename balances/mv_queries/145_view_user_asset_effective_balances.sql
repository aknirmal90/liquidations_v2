-- Create view for user asset effective balances with interest accrual
-- This view calculates effective debt and collateral per user and asset
-- Used as input for view_user_health_factor
--
-- Effective Collateral calculation (per asset):
-- - balance * liquidation_threshold / 10000 / decimals_places * is_collateral_enabled * price / decimals_places * interest_accrual_factor
-- - liquidation_threshold depends on eMode status (eModeLiquidationThreshold vs collateralLiquidationThreshold)
--
-- Effective Debt calculation (per asset):
-- - debt_balance / decimals_places * price / decimals_places * interest_accrual_factor
--
-- Interest Accrual Factor:
-- - Accounts for interest accrued between last index update and latest block
-- - Factor = 1 + (interest_rate / RAY / seconds_in_year * (latest_block - updated_at_block) * 12)

CREATE VIEW IF NOT EXISTS aave_ethereum.view_user_asset_effective_balances AS
WITH
-- Get latest network block info
network_info AS (
    SELECT
        dictGetOrDefault('aave_ethereum.NetworkBlockInfoDictionary', 'latest_block_number', toUInt8(1), toUInt64(0)) AS latest_block_number
),
-- Get current underlying balances by applying liquidity index to scaled balances
-- Uses in-memory table for fast access (filtered to only non-zero balances)
current_balances AS (
    SELECT
        lb.user,
        lb.asset,
        -- Convert scaled balance to underlying: floor((scaled * liquidityIndex) / RAY)
        -- RAY = 1e27
        floor((toInt256(lb.collateral_scaled_balance) * toInt256(dictGetOrDefault('aave_ethereum.dict_collateral_liquidity_index', 'liquidityIndex', lb.asset, toUInt256(0)))) / toInt256('1000000000000000000000000000')) AS collateral_balance,
        floor((toInt256(lb.variable_debt_scaled_balance) * toInt256(dictGetOrDefault('aave_ethereum.dict_debt_liquidity_index', 'liquidityIndex', lb.asset, toUInt256(0)))) / toInt256('1000000000000000000000000000')) AS debt_balance,
        -- Get interest rates and updated block numbers for accrual calculation
        dictGetOrDefault('aave_ethereum.dict_collateral_liquidity_index', 'interest_rate', lb.asset, toUInt256(0)) AS collateral_interest_rate,
        dictGetOrDefault('aave_ethereum.dict_collateral_liquidity_index', 'updated_at_block', lb.asset, toUInt64(0)) AS collateral_updated_at_block,
        dictGetOrDefault('aave_ethereum.dict_debt_liquidity_index', 'interest_rate', lb.asset, toUInt256(0)) AS debt_interest_rate,
        dictGetOrDefault('aave_ethereum.dict_debt_liquidity_index', 'updated_at_block', lb.asset, toUInt64(0)) AS debt_updated_at_block
    FROM aave_ethereum.LatestBalances_v2_Memory AS lb
    CROSS JOIN network_info
)
-- Calculate effective collateral and debt per user per asset
SELECT
    cb.user,
    cb.asset,
    cb.collateral_balance,
    cb.debt_balance,

    -- Get user's eMode status (default 0 if not in dict)
    dictGetOrDefault('aave_ethereum.dict_emode_status', 'is_enabled_in_emode', toString(cb.user), toInt8(0)) AS is_in_emode,

    -- Get asset configuration
    dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'decimals_places', cb.asset, toUInt256(1)) AS decimals_places,
    dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'historical_event_price', cb.asset, toFloat64(0)) AS price,
    dictGetOrDefault('aave_ethereum.dict_collateral_status', 'is_enabled_as_collateral', tuple(cb.user, cb.asset), toInt8(0)) AS is_collateral_enabled,

    -- Liquidation thresholds
    dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'eModeLiquidationThreshold', cb.asset, toUInt256(0)) AS emode_liquidation_threshold,
    dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'collateralLiquidationThreshold', cb.asset, toUInt256(0)) AS collateral_liquidation_threshold,

    -- Interest accrual factors
    cb.collateral_interest_rate,
    cb.collateral_updated_at_block,
    (SELECT latest_block_number FROM network_info) AS latest_block_number,
    -- Collateral interest accrual factor: 1 + (interest_rate / RAY / seconds_in_year * (latest_block - updated_at_block) * 12)
    (1.0 + (CAST(cb.collateral_interest_rate AS Float64) / 1e27 / 31536000.0 * CAST((SELECT latest_block_number FROM network_info) - cb.collateral_updated_at_block AS Float64) * 12.0)) AS collateral_interest_accrual_factor,

    cb.debt_interest_rate,
    cb.debt_updated_at_block,
    -- Debt interest accrual factor: 1 + (interest_rate / RAY / seconds_in_year * (latest_block - updated_at_block) * 12)
    (1.0 + (CAST(cb.debt_interest_rate AS Float64) / 1e27 / 31536000.0 * CAST((SELECT latest_block_number FROM network_info) - cb.debt_updated_at_block AS Float64) * 12.0)) AS debt_interest_accrual_factor,

    -- Effective Collateral (per asset):
    -- For eMode users: use eModeLiquidationThreshold
    -- For non-eMode users: use collateralLiquidationThreshold
    -- Includes interest accrual factor
    floor(
        CAST(cb.collateral_balance AS Float64) *
        CAST(
            if(
                dictGetOrDefault('aave_ethereum.dict_emode_status', 'is_enabled_in_emode', cb.user, toInt8(0)) = 1,
                dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'eModeLiquidationThreshold', cb.asset, toUInt256(0)),
                dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'collateralLiquidationThreshold', cb.asset, toUInt256(0))
            ) AS Float64
        ) / 10000.0 *
        CAST(dictGetOrDefault('aave_ethereum.dict_collateral_status', 'is_enabled_as_collateral', tuple(cb.user, cb.asset), toInt8(0)) AS Float64) *
        CAST(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'historical_event_price', cb.asset, toFloat64(0)) AS Float64) /
        CAST(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'decimals_places', cb.asset, toUInt256(1)) AS Float64) *
        -- Interest accrual factor for collateral
        (1.0 + (CAST(cb.collateral_interest_rate AS Float64) / 1e27 / 31536000.0 * CAST((SELECT latest_block_number FROM network_info) - cb.collateral_updated_at_block AS Float64) * 12.0))
    ) AS effective_collateral,

    -- Effective Debt (per asset):
    -- debt_balance * price / decimals * interest_accrual_factor
    ceil(
        CAST(cb.debt_balance AS Float64) *
        CAST(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'historical_event_price', cb.asset, toFloat64(0)) AS Float64) /
        CAST(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'decimals_places', cb.asset, toUInt256(1)) AS Float64) *
        -- Interest accrual factor for debt
        (1.0 + (CAST(cb.debt_interest_rate AS Float64) / 1e27 / 31536000.0 * CAST((SELECT latest_block_number FROM network_info) - cb.debt_updated_at_block AS Float64) * 12.0))
    ) AS effective_debt

FROM current_balances AS cb
WHERE cb.collateral_balance != 0 OR cb.debt_balance != 0;

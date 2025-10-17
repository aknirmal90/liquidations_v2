-- Create view for user health factor calculation
-- Health Factor = Effective Collateral / Effective Debt
--
-- Effective Collateral calculation:
-- - If user is in eMode: sum(balance * eModeLiquidationThreshold / 10000 / decimals_places * is_collateral_enabled * historical_event_price / decimals_places)
-- - If user is NOT in eMode: sum(balance * collateralLiquidationThreshold / 10000 / decimals_places * is_collateral_enabled * historical_event_price / decimals_places)
--
-- Effective Debt calculation:
-- - sum(debt_balance / decimals_places * historical_event_price / decimals_places)

CREATE VIEW IF NOT EXISTS aave_ethereum.view_user_health_factor AS
WITH
-- Get current underlying balances by applying liquidity index to scaled balances
-- Uses in-memory table for fast access (filtered to only non-zero balances)
current_balances AS (
    SELECT
        lb.user,
        lb.asset,
        -- Convert scaled balance to underlying: floor((scaled * liquidityIndex) / RAY)
        -- RAY = 1e27
        floor((toInt256(lb.collateral_scaled_balance) * toInt256(dictGet('aave_ethereum.dict_latest_asset_configuration', 'max_collateral_liquidityIndex', lb.asset))) / toInt256('1000000000000000000000000000')) AS collateral_balance,
        floor((toInt256(lb.variable_debt_scaled_balance) * toInt256(dictGet('aave_ethereum.dict_latest_asset_configuration', 'max_variable_debt_liquidityIndex', lb.asset))) / toInt256('1000000000000000000000000000')) AS debt_balance
    FROM aave_ethereum.LatestBalances_v2_Memory AS lb
),
-- Calculate effective collateral and debt per user
user_metrics AS (
    SELECT
        cb.user,
        -- Get user's eMode status (default 0 if not in dict)
        dictGetOrDefault('aave_ethereum.dict_emode_status', 'is_enabled_in_emode', cb.user, toInt8(0)) AS is_in_emode,

        -- Effective Collateral:
        -- For eMode users: use eModeLiquidationThreshold
        -- For non-eMode users: use collateralLiquidationThreshold
        sumIf(
            CAST(cb.collateral_balance AS Float64) *
            CAST(
                if(
                    dictGetOrDefault('aave_ethereum.dict_emode_status', 'is_enabled_in_emode', cb.user, toInt8(0)) = 1,
                    dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'eModeLiquidationThreshold', cb.asset, toUInt256(0)),
                    dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'collateralLiquidationThreshold', cb.asset, toUInt256(0))
                ) AS Float64
            ) / 10000.0 /
            CAST(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'decimals_places', cb.asset, toUInt256(1)) AS Float64) *
            CAST(dictGetOrDefault('aave_ethereum.dict_collateral_status', 'is_enabled_as_collateral', tuple(cb.user, cb.asset), toInt8(0)) AS Float64) *
            CAST(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'historical_event_price', cb.asset, toFloat64(0)) AS Float64) /
            CAST(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'decimals_places', cb.asset, toUInt256(1)) AS Float64),
            cb.collateral_balance > 0
        ) AS effective_collateral,

        -- Effective Debt: sum of all debt balances divided by decimals twice and multiplied by price
        sum(
            CAST(cb.debt_balance AS Float64) /
            CAST(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'decimals_places', cb.asset, toUInt256(1)) AS Float64) *
            CAST(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'historical_event_price', cb.asset, toFloat64(0)) AS Float64) /
            CAST(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'decimals_places', cb.asset, toUInt256(1)) AS Float64)
        ) AS effective_debt

    FROM current_balances AS cb
    WHERE cb.collateral_balance != 0 OR cb.debt_balance != 0
    GROUP BY cb.user
)
SELECT
    user,
    is_in_emode,
    effective_collateral,
    effective_debt,
    -- Health Factor = Effective Collateral / Effective Debt
    -- If debt is 0, health factor is infinite (represented as -1 for practical purposes)
    -- If collateral is 0 and debt > 0, health factor is 0 (liquidatable)
    if(
        effective_debt = 0,
        999.9,
        effective_collateral / effective_debt
    ) AS health_factor
FROM user_metrics;

-- Create view for liquidation candidates
-- Identifies profitable liquidation opportunities based on:
-- 1. Health factor between 0.9 and 1.25
-- 2. Effective collateral and debt > $10,000
-- 3. Priority debt assets (WETH, USDT, USDC, wstETH, USDe, WBTC)
-- 4. Selects best collateral asset based on profit calculation
--
-- This view uses accrued balances from view_user_asset_effective_balances (query 145)
-- and calculates effective values inline using asset metadata from dictionaries
--
-- IMPORTANT: collateral_balance and debt_balance are ACCRUED values (with interest applied)
-- to match on-chain currentATokenBalance and currentVariableDebt for accurate comparisons

CREATE VIEW IF NOT EXISTS aave_ethereum.view_liquidation_candidates AS
WITH
-- Priority assets for liquidation (debt assets we want to target)
priority_debt_assets AS (
    SELECT asset
    FROM (
        SELECT '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS asset  -- WETH
        UNION ALL SELECT '0xdac17f958d2ee523a2206206994597c13d831ec7'  -- USDT
        UNION ALL SELECT '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'  -- USDC
        UNION ALL SELECT '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0'  -- wstETH
        UNION ALL SELECT '0x4c9edd5852cd905f086c759e8383e09bff1e68b3'  -- USDe
        UNION ALL SELECT '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'  -- WBTC
    )
),

-- Get users with health factors in liquidation range
at_risk_users AS (
    SELECT
        user,
        health_factor,
        effective_collateral,
        effective_debt,
        is_in_emode
    FROM aave_ethereum.view_user_health_factor
    WHERE health_factor > 1.0
        AND health_factor <= 1.25
        AND effective_collateral > 10000
        AND effective_debt > 10000
),

-- Get user asset balances for at-risk users
user_positions AS (
    SELECT
        eb.user,
        eb.asset,
        eb.collateral_balance,
        eb.debt_balance,
        eb.accrued_collateral_balance,
        eb.accrued_debt_balance,
        -- Fetch asset metadata from dictionaries
        dictGetOrDefault('aave_ethereum.dict_emode_status', 'is_enabled_in_emode', toString(eb.user), toInt8(0)) AS is_in_emode,
        dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'decimals_places', eb.asset, toUInt256(1)) AS decimals_places,
        dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'historical_event_price_usd', eb.asset, toFloat64(0)) AS price,
        dictGetOrDefault('aave_ethereum.dict_collateral_status', 'is_enabled_as_collateral', tuple(eb.user, eb.asset), toInt8(0)) AS is_collateral_enabled,
        dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'eModeLiquidationThreshold', eb.asset, toUInt256(0)) AS emode_liquidation_threshold,
        dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'collateralLiquidationThreshold', eb.asset, toUInt256(0)) AS collateral_liquidation_threshold,
        ar.health_factor,
        ar.effective_collateral AS total_effective_collateral,
        ar.effective_debt AS total_effective_debt
    FROM aave_ethereum.view_user_asset_effective_balances AS eb
    INNER JOIN at_risk_users AS ar ON eb.user = ar.user
    WHERE eb.collateral_balance > 0 OR eb.debt_balance > 0
),

-- Calculate collateral opportunities with profit
collateral_opportunities AS (
    SELECT
        up.user,
        up.asset AS collateral_asset,
        up.collateral_balance,
        up.accrued_collateral_balance,
        -- Calculate effective collateral value using accrued balance
        cast(floor(
            toFloat64(up.accrued_collateral_balance)
            * toFloat64(
                if(
                    up.is_in_emode = 1,
                    up.emode_liquidation_threshold,
                    up.collateral_liquidation_threshold
                )
            )
            * toFloat64(up.is_collateral_enabled)
            * up.price
            / (10000 * toFloat64(up.decimals_places))
        ) as UInt256) AS collateral_effective_value,
        up.price AS collateral_price,
        up.decimals_places AS collateral_decimals,
        up.health_factor,
        up.total_effective_collateral,
        up.total_effective_debt,
        up.is_in_emode,

        -- Get liquidation bonus (use eMode bonus if in eMode, otherwise collateral bonus)
        if(
            up.is_in_emode = 1,
            dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'eModeLiquidationBonus', up.asset, toUInt256(10000)),
            dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'collateralLiquidationBonus', up.asset, toUInt256(10000))
        ) AS liquidation_bonus,

        -- Check if this is a priority asset
        if(up.asset IN (SELECT asset FROM priority_debt_assets), 1, 0) AS is_priority_asset,

        -- Calculate profit: (liquidation_bonus / 10000.0 - 1) * accrued_collateral_balance * price / decimals
        (
            (toFloat64(
                if(
                    up.is_in_emode = 1,
                    dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'eModeLiquidationBonus', up.asset, toUInt256(10000)),
                    dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'collateralLiquidationBonus', up.asset, toUInt256(10000))
                )
            ) / 10000.0 - 1.0)
            * toFloat64(up.accrued_collateral_balance)
            * up.price
            / toFloat64(up.decimals_places)
        ) AS profit

    FROM user_positions AS up
    WHERE up.collateral_balance > 0
        AND up.is_collateral_enabled = 1
),

-- Get debt positions for at-risk users
debt_positions AS (
    SELECT
        up.user,
        up.asset AS debt_asset,
        up.debt_balance,
        up.accrued_debt_balance,
        -- Calculate effective debt value using accrued balance
        cast(floor(
            toFloat64(up.accrued_debt_balance)
            * up.price
            / toFloat64(up.decimals_places)
        ) as UInt256) AS debt_effective_value,
        up.price AS debt_price,
        up.decimals_places AS debt_decimals,
        if(up.asset IN (SELECT asset FROM priority_debt_assets), 1, 0) AS is_priority_debt
    FROM user_positions AS up
    WHERE up.debt_balance > 0
),

-- Combine collateral and debt to find best liquidation opportunities
liquidation_pairs AS (
    SELECT
        co.user,
        co.collateral_asset,
        dp.debt_asset,
        co.accrued_collateral_balance,
        dp.accrued_debt_balance,
        co.collateral_price,
        dp.debt_price,
        co.collateral_decimals,
        dp.debt_decimals,
        co.liquidation_bonus,
        co.profit,
        co.health_factor,
        co.collateral_effective_value AS total_effective_collateral,
        co.debt_effective_value AS total_effective_debt,
        co.is_priority_asset AS is_priority_collateral,
        dp.is_priority_debt,

        -- Calculate maximum debt that can be covered (50% of total accrued debt)
        toFloat64(dp.accrued_debt_balance) * 0.5 AS max_debt_to_cover,

        -- Rank collateral assets for each user-debt pair
        -- Priority: 1) Priority collateral assets, 2) Highest profit
        ROW_NUMBER() OVER (
            PARTITION BY co.user, dp.debt_asset
            ORDER BY
                co.is_priority_asset DESC,
                co.profit DESC
        ) AS collateral_rank

    FROM collateral_opportunities AS co
    INNER JOIN debt_positions AS dp ON co.user = dp.user
    WHERE dp.is_priority_debt = 1  -- Only consider priority debt assets
)

-- Select best liquidation opportunity per user-debt pair
SELECT
    user,
    collateral_asset,
    debt_asset,
    max_debt_to_cover AS debt_to_cover,
    profit,
    health_factor,
    total_effective_collateral AS effective_collateral,
    total_effective_debt AS effective_debt,
    toFloat64(accrued_collateral_balance) AS collateral_balance,
    toFloat64(accrued_debt_balance) AS debt_balance,
    liquidation_bonus,
    collateral_price,
    debt_price,
    collateral_decimals,
    debt_decimals,
    is_priority_debt,
    is_priority_collateral
FROM liquidation_pairs
WHERE collateral_rank = 1  -- Only take the best collateral for each debt
    AND profit > 0  -- Only profitable liquidations
ORDER BY profit DESC;

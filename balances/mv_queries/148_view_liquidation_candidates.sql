-- Create view for liquidation candidates
-- Identifies profitable liquidation opportunities based on:
-- 1. Health factor between 0.9 and 1.25
-- 2. Effective collateral and debt > $10,000
-- 3. ALL debt assets (no priority filtering)
-- 4. Selects best collateral asset based on profit calculation
--
-- This view uses accrued balances from view_user_asset_effective_balances (query 145)
-- and calculates effective values inline using asset metadata from dictionaries
--
-- IMPORTANT: collateral_balance and debt_balance are ACCRUED values (with interest applied)
-- to match on-chain currentATokenBalance and currentVariableDebt for accurate comparisons

CREATE VIEW IF NOT EXISTS aave_ethereum.view_liquidation_candidates AS
WITH
-- Get users with health factors in liquidation range
at_risk_users AS (
    SELECT
        user,
        health_factor,
        effective_collateral_usd,
        effective_debt_usd,
        is_in_emode
    FROM aave_ethereum.view_user_health_factor
    WHERE health_factor > toDecimal256(1.0, 18)
        AND health_factor <= toDecimal256(1.25, 18)
        AND effective_collateral_usd > toDecimal256(10000, 18)
        AND effective_debt_usd > toDecimal256(10000, 18)
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
        ar.effective_collateral_usd AS total_effective_collateral,
        ar.effective_debt_usd AS total_effective_debt
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
        (
            up.accrued_collateral_balance
            * toDecimal256(
                if(
                    up.is_in_emode = 1,
                    up.emode_liquidation_threshold,
                    up.collateral_liquidation_threshold
                ), 0
            )
            * toDecimal256(up.is_collateral_enabled, 0)
            * toDecimal256(up.price, 18)
            / (toDecimal256(10000, 0) * toDecimal256(up.decimals_places, 0))
        ) AS collateral_effective_value,
        up.price AS collateral_price,
        up.decimals_places AS collateral_decimals,
        up.health_factor,
        up.is_in_emode,

        -- Get liquidation bonus (use eMode bonus if in eMode, otherwise collateral bonus)
        if(
            up.is_in_emode = 1,
            dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'eModeLiquidationBonus', up.asset, toUInt256(10000)),
            dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'collateralLiquidationBonus', up.asset, toUInt256(10000))
        ) AS liquidation_bonus,

        -- Calculate profit: (liquidation_bonus / 10000.0 - 1) * accrued_collateral_balance * price / decimals
        (
            (toDecimal256(
                if(
                    up.is_in_emode = 1,
                    dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'eModeLiquidationBonus', up.asset, toUInt256(10000)),
                    dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'collateralLiquidationBonus', up.asset, toUInt256(10000))
                ), 0
            ) / toDecimal256(10000.0, 18) - toDecimal256(1.0, 18))
            * up.accrued_collateral_balance
            * toDecimal256(up.price, 18)
            / toDecimal256(up.decimals_places, 0)
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
        (
            up.accrued_debt_balance
            * toDecimal256(up.price, 18)
            / toDecimal256(up.decimals_places, 0)
        ) AS debt_effective_value,
        up.price AS debt_price,
        up.decimals_places AS debt_decimals
    FROM user_positions AS up
    WHERE up.debt_balance > 0
),

-- Combine collateral and debt to find best liquidation opportunities
liquidation_pairs AS (
    SELECT
        co.user,
        co.collateral_asset,
        dp.debt_asset,
        co.collateral_balance,
        dp.debt_balance,
        co.collateral_price,
        dp.debt_price,
        co.collateral_decimals,
        dp.debt_decimals,
        co.liquidation_bonus,
        co.profit,
        co.health_factor,
        co.collateral_effective_value,
        dp.debt_effective_value,

        -- Calculate maximum debt that can be covered (50% of total accrued debt)
        dp.debt_balance * toDecimal256(0.5, 18) AS max_debt_to_cover,

        -- Rank collateral assets for each user-debt pair
        -- Priority: Highest profit
        ROW_NUMBER() OVER (
            PARTITION BY co.user, dp.debt_asset
            ORDER BY
                co.profit DESC
        ) AS collateral_rank

    FROM collateral_opportunities AS co
    INNER JOIN debt_positions AS dp ON co.user = dp.user
)

-- Select best liquidation opportunity per user-debt pair
SELECT
    user,
    collateral_asset,
    debt_asset,
    max_debt_to_cover AS debt_to_cover,
    profit,
    health_factor,
    collateral_effective_value AS effective_collateral,
    debt_effective_value AS effective_debt,
    collateral_balance,
    debt_balance,
    liquidation_bonus,
    collateral_price,
    debt_price,
    collateral_decimals,
    debt_decimals
FROM liquidation_pairs
WHERE collateral_rank = 1  -- Only take the best collateral for each debt
    AND profit > 0  -- Only profitable liquidations
ORDER BY profit DESC;

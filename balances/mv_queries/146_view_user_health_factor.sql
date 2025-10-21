-- Create view for user health factor calculation
-- Health Factor = Effective Collateral / Effective Debt
--
-- This view leverages accrued balances from view_user_asset_effective_balances (query 145)
-- and applies business logic for eMode, liquidation thresholds, prices, and collateral status.
--
-- Effective Collateral calculation (per asset):
-- - (accrued_collateral_balance * liquidation_threshold * is_collateral_enabled * price) / (10000 * decimals_places)
-- - liquidation_threshold depends on eMode status (eModeLiquidationThreshold vs collateralLiquidationThreshold)
--
-- Effective Debt calculation (per asset):
-- - (accrued_debt_balance * price) / decimals_places

CREATE VIEW IF NOT EXISTS aave_ethereum.view_user_health_factor AS
WITH
asset_effective_balances AS (
    SELECT
        uaeb.user,
        uaeb.asset,
        uaeb.accrued_collateral_balance,
        uaeb.accrued_debt_balance,
        -- eMode status
        dictGetOrDefault('aave_ethereum.dict_emode_status', 'is_enabled_in_emode', toString(uaeb.user), toInt8(0)) AS is_in_emode,
        -- Asset configuration
        dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'decimals_places', uaeb.asset, toUInt256(1)) AS decimals_places,
        dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'historical_event_price', uaeb.asset, toFloat64(0)) AS price,
        dictGetOrDefault('aave_ethereum.dict_collateral_status', 'is_enabled_as_collateral', tuple(uaeb.user, uaeb.asset), toInt8(0)) AS is_collateral_enabled,
        -- Liquidation thresholds
        dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'eModeLiquidationThreshold', uaeb.asset, toUInt256(0)) AS emode_liquidation_threshold,
        dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'collateralLiquidationThreshold', uaeb.asset, toUInt256(0)) AS collateral_liquidation_threshold
    FROM aave_ethereum.view_user_asset_effective_balances AS uaeb
),
effective_balances AS (
    SELECT
        user,
        asset,
        is_in_emode,
        accrued_collateral_balance,
        accrued_debt_balance,
        decimals_places,
        -- Effective Collateral: apply liquidation threshold based on eMode, collateral status, and price
        cast(floor(
            toFloat64(accrued_collateral_balance)
            * toFloat64(
                if(
                    is_in_emode = 1,
                    emode_liquidation_threshold,
                    collateral_liquidation_threshold
                )
            )
            * toFloat64(is_collateral_enabled)
            * price
            / (10000 * toFloat64(decimals_places))
        ) as UInt256) AS effective_collateral,
        -- Effective Debt: apply price adjustment
        cast(floor(
            toFloat64(accrued_debt_balance)
            * price
            / (toFloat64(decimals_places))
        ) as UInt256) AS effective_debt
    FROM asset_effective_balances
),
effective_balances_usd AS (
    SELECT
        user,
        asset,
        is_in_emode,
        accrued_collateral_balance,
        accrued_debt_balance,
        decimals_places,
        effective_collateral,
        effective_debt,
        effective_collateral / decimals_places AS effective_collateral_usd,
        effective_debt / decimals_places AS effective_debt_usd
    FROM effective_balances
),
user_totals AS (
    SELECT
        user,
        is_in_emode,
        sum(accrued_collateral_balance) AS total_accrued_collateral_balance,
        sum(accrued_debt_balance) AS total_accrued_debt_balance,
        sum(effective_collateral) AS total_effective_collateral,
        sum(effective_debt) AS total_effective_debt,
        sum(effective_collateral_usd) AS total_effective_collateral_usd,
        sum(effective_debt_usd) AS total_effective_debt_usd
    FROM effective_balances_usd
    GROUP BY user, is_in_emode
)
SELECT
    user,
    is_in_emode,
    total_accrued_collateral_balance,
    total_accrued_debt_balance,
    total_effective_collateral_usd AS effective_collateral_usd,
    total_effective_debt_usd AS effective_debt_usd,
    -- Health Factor = Effective Collateral / Effective Debt
    -- If debt is 0, health factor is infinite (represented as 999.9 for practical purposes)
    -- If collateral is 0 and debt > 0, health factor is 0 (liquidatable)
    if(
        total_effective_debt = 0,
        999.9,
        total_effective_collateral / total_effective_debt
    ) AS health_factor
FROM user_totals;

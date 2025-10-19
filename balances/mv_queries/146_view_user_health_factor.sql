-- Create view for user health factor calculation
-- Health Factor = Effective Collateral / Effective Debt
--
-- This view leverages calculations from view_user_asset_effective_balances (query 145)
-- to avoid duplicating balance and interest accrual factor calculations.
--
-- Balances are calculated as:
-- - Collateral: FLOOR(collateral_balance * collateral_interest_accrual_factor)
-- - Debt: CEIL(debt_balance * debt_interest_accrual_factor)

CREATE VIEW IF NOT EXISTS aave_ethereum.view_user_health_factor AS
WITH user_totals AS (
    SELECT
        user,
        is_in_emode,
        sum(effective_collateral) AS total_effective_collateral,
        sum(effective_debt) AS total_effective_debt
    FROM aave_ethereum.view_user_asset_effective_balances
    GROUP BY user, is_in_emode
)
SELECT
    user,
    is_in_emode,
    floor(total_effective_collateral) AS effective_collateral,
    ceil(total_effective_debt) AS effective_debt,
    -- Health Factor = Effective Collateral / Effective Debt
    -- If debt is 0, health factor is infinite (represented as 999.9 for practical purposes)
    -- If collateral is 0 and debt > 0, health factor is 0 (liquidatable)
    if(
        ceil(total_effective_debt) = 0,
        999.9,
        floor(total_effective_collateral) / ceil(total_effective_debt)
    ) AS health_factor
FROM user_totals;

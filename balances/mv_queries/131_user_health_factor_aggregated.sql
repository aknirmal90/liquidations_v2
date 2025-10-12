-- Query to get aggregated health factor for a user
-- This aggregates the per-asset values to get overall user health factor

SELECT
    user,
    sum(effective_collateral_value_usd) as total_effective_collateral_value_usd,
    sum(debt_value_usd) as total_debt_value_usd,
    CASE
        WHEN sum(debt_value_usd) > 0 THEN
            sum(effective_collateral_value_usd) / sum(debt_value_usd)
        ELSE 999999.0
    END as health_factor
FROM aave_ethereum.view_UserHealthFactor
GROUP BY user
HAVING health_factor <= 1.5
LIMIT 100
;


SELECT
    user,
    sum(effective_collateral_value_usd) as total_effective_collateral_value_usd,
    sum(debt_value_usd) as total_debt_value_usd,
    CASE
        WHEN sum(debt_value_usd) > 0 THEN
            sum(effective_collateral_value_usd) / sum(debt_value_usd)
        ELSE 999999.0
    END as health_factor
FROM aave_ethereum.view_UserHealthFactor
GROUP BY user
HAVING total_effective_collateral_value_usd > 0 and total_debt_value_usd > 0
LIMIT 100
;

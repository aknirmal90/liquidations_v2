-- Materialized view to populate DebtLiquidityIndex from Burn events
-- Only inserts variable debt-type events with blockNumber as version
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_debt_liquidity_index_from_burn
TO aave_ethereum.DebtLiquidityIndex
AS SELECT
    asset,
    index as liquidityIndex,
    index as version
FROM aave_ethereum.Burn
WHERE type = 'VariableDebt';

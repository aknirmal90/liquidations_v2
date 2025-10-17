-- Materialized view to populate DebtLiquidityIndex from Mint events
-- Only inserts variable debt-type events with blockNumber as version
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_debt_liquidity_index_from_mint
TO aave_ethereum.DebtLiquidityIndex
AS SELECT
    asset,
    index as liquidityIndex,
    index as version
FROM aave_ethereum.Mint
WHERE type = 'VariableDebt';

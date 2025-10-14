-- Materialized view to populate MaxLiquidityIndex table
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_max_liquidity_index TO aave_ethereum.MaxLiquidityIndex
AS SELECT
    asset,
    maxMerge(collateral_liquidityIndex) as max_collateral_liquidityIndex,
    maxMerge(variable_debt_liquidityIndex) as max_variable_debt_liquidityIndex,
    now64() as last_updated
FROM aave_ethereum.LatestBalances
GROUP BY asset;

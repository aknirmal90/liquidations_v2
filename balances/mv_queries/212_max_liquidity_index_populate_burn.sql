-- Materialized view to populate MaxLiquidityIndex table from Burn events
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_max_liquidity_index_from_burn TO aave_ethereum.MaxLiquidityIndex
AS SELECT
    asset,
    maxSimpleState(CASE WHEN type = 'Collateral' THEN index ELSE toUInt256(0) END) as max_collateral_liquidityIndex,
    maxSimpleState(CASE WHEN type = 'VariableDebt' THEN index ELSE toUInt256(0) END) as max_variable_debt_liquidityIndex,
    now64() as last_updated
FROM aave_ethereum.Burn
GROUP BY asset;

-- Materialized view to populate MaxLiquidityIndex table from Mint and Burn events
-- This tracks the maximum (current) liquidity index per asset across all events
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_max_liquidity_index_from_mint TO aave_ethereum.MaxLiquidityIndex
AS SELECT
    asset,
    maxSimpleState(CASE WHEN type = 'Collateral' THEN index ELSE toUInt256(0) END) as max_collateral_liquidityIndex,
    maxSimpleState(CASE WHEN type = 'VariableDebt' THEN index ELSE toUInt256(0) END) as max_variable_debt_liquidityIndex,
    now64() as last_updated
FROM aave_ethereum.Mint
GROUP BY asset;

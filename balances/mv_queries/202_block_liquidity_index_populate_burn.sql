-- Materialized view to populate BlockLiquidityIndex from Burn events
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_block_liquidity_index_from_burn
TO aave_ethereum.BlockLiquidityIndex
AS SELECT
    asset,
    blockNumber as block_number,
    maxSimpleState(CASE WHEN type = 'Collateral' THEN index ELSE toUInt256(0) END) as max_collateral_liquidityIndex,
    maxSimpleState(CASE WHEN type = 'VariableDebt' THEN index ELSE toUInt256(0) END) as max_variable_debt_liquidityIndex
FROM aave_ethereum.Burn
GROUP BY asset, blockNumber;

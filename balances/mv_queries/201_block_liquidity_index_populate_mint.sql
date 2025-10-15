-- Materialized view to populate BlockLiquidityIndex from Mint and Burn events
-- These events contain the liquidity index at the time of the event
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_block_liquidity_index_from_mint
TO aave_ethereum.BlockLiquidityIndex
AS SELECT
    asset,
    blockNumber as block_number,
    maxSimpleState(CASE WHEN type = 'Collateral' THEN index ELSE toUInt256(0) END) as max_collateral_liquidityIndex,
    maxSimpleState(CASE WHEN type = 'VariableDebt' THEN index ELSE toUInt256(0) END) as max_variable_debt_liquidityIndex
FROM aave_ethereum.Mint
GROUP BY asset, blockNumber;

-- Materialized view to populate CollateralLiquidityIndex from ReserveDataUpdated events
-- Uses liquidityIndex from ReserveDataUpdated with composite version (block * 1B + txIndex * 10k + logIndex)
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_collateral_liquidity_index
TO aave_ethereum.CollateralLiquidityIndex
AS SELECT
    reserve as asset,
    liquidityIndex,
    (blockNumber * 1000000000 + transactionIndex * 10000 + logIndex) AS version
FROM aave_ethereum.ReserveDataUpdated;

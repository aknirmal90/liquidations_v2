-- Materialized view to populate DebtLiquidityIndex from ReserveDataUpdated events
-- Uses variableBorrowIndex from ReserveDataUpdated with composite version (block * 1B + txIndex * 10k + logIndex)
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_debt_liquidity_index
TO aave_ethereum.DebtLiquidityIndex
AS SELECT
    reserve as asset,
    variableBorrowIndex as liquidityIndex,
    (blockNumber * 1000000000 + transactionIndex * 10000 + logIndex) AS version
FROM aave_ethereum.ReserveDataUpdated;

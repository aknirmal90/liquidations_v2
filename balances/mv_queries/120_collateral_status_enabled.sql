-- Materialized view for ReserveUsedAsCollateralEnabled events
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_collateral_status_enabled TO aave_ethereum.CollateralStatusDictionary
AS SELECT
    user,
    reserve as asset,
    toInt8(1) as is_enabled_as_collateral,
    blockNumber,
    transactionHash,
    logIndex,
    blockTimestamp,
    (blockNumber * 1000000000 + transactionIndex * 10000 + logIndex) AS version
FROM aave_ethereum.ReserveUsedAsCollateralEnabled;

-- Materialized view for ReserveUsedAsCollateralDisabled events
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_collateral_status_disabled TO aave_ethereum.CollateralStatusDictionary
AS SELECT
    user,
    reserve as asset,
    toInt8(0) as is_enabled_as_collateral,
    blockNumber,
    transactionHash,
    logIndex,
    blockTimestamp,
    (blockNumber * 1000000000 + transactionIndex * 10000 + logIndex) AS version
FROM aave_ethereum.ReserveUsedAsCollateralDisabled;

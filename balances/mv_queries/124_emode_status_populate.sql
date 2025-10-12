-- Materialized view to populate eMode status from UserEModeSet events
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_emode_status_populate TO aave_ethereum.EModeStatusDictionary
AS SELECT
    user,
    CASE
        WHEN categoryId > 0 THEN toInt8(1)
        ELSE toInt8(0)
    END as is_enabled_in_emode,
    blockNumber,
    transactionHash,
    logIndex,
    blockTimestamp,
    (blockNumber * 1000000000 + transactionIndex * 10000 + logIndex) AS version
FROM aave_ethereum.UserEModeSet;

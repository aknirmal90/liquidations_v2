CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_event_raw_numerator_latest
TO aave_ethereum.PriceLatestEventRawNumerator
AS
SELECT
    asset,
    asset_source,
    name,
    blockTimestamp,
    blockNumber,
    transactionHash,
    type,
    numerator
FROM aave_ethereum.EventRawNumerator;

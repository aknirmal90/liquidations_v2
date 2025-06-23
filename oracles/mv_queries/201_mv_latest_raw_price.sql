CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_latest_raw_price
TO aave_ethereum.LatestRawPriceEvent
AS
SELECT
    asset,
    price,
    transactionHash,
    blockNumber,
    transactionIndex,
    logIndex,
    blockTimestamp,
    (blockNumber * 1000000000 + transactionIndex * 10000 + logIndex) AS version
FROM aave_ethereum.RawPriceEvent;

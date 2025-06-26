CREATE TABLE IF NOT EXISTS aave_ethereum.LatestRawPriceEvent
(
    asset String,
    price UInt256,  -- adjust to Int64 if UInt256 unsupported
    transactionHash String,
    blockNumber UInt64,
    transactionIndex UInt32,
    logIndex UInt32,
    blockTimestamp DateTime64(0),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (asset);

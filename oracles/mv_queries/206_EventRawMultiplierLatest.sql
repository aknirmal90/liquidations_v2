CREATE TABLE IF NOT EXISTS aave_ethereum.PriceLatestEventRawMultiplier
(
    asset String,
    asset_source String,
    name String,
    blockTimestamp DateTime64(6),
    blockNumber UInt64,
    multiplier UInt256
)
ENGINE = ReplacingMergeTree(blockTimestamp)
ORDER BY asset;

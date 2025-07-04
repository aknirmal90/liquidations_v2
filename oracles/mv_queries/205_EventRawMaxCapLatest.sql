CREATE TABLE IF NOT EXISTS aave_ethereum.PriceLatestEventRawMaxCap
(
    asset String,
    asset_source String,
    name String,
    blockTimestamp DateTime64(6),
    blockNumber UInt64,
    max_cap UInt256
)
ENGINE = ReplacingMergeTree(blockTimestamp)
ORDER BY asset;

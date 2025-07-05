CREATE TABLE IF NOT EXISTS aave_ethereum.PriceLatestEventRawMaxCap
(
    asset String,
    asset_source String,
    name String,
    blockTimestamp DateTime64(6),
    blockNumber UInt64,
    max_cap Float64,
    max_cap_type UInt8
)
ENGINE = ReplacingMergeTree(blockTimestamp)
ORDER BY asset;

CREATE TABLE IF NOT EXISTS aave_ethereum.PriceLatestEventRawMaxCap
(
    asset String,
    asset_source String,
    timestamp DateTime64(0),
    max_cap UInt256
)
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY asset;

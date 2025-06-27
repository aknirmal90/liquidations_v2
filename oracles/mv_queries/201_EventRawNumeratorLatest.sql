CREATE TABLE IF NOT EXISTS aave_ethereum.PriceLatestEventRawNumerator
(
    asset String,
    asset_source String,
    timestamp DateTime64(0),
    numerator UInt256
)
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY asset;

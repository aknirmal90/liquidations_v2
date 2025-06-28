CREATE TABLE IF NOT EXISTS aave_ethereum.PriceLatestEventRawDenominator
(
    asset String,
    asset_source String,
    name String,
    timestamp DateTime64(6),
    denominator UInt256
)
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY asset;

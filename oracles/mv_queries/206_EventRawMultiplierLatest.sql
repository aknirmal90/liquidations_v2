CREATE TABLE IF NOT EXISTS aave_ethereum.PriceLatestEventRawMultiplier
(
    asset String,
    asset_source String,
    timestamp DateTime64(0),
    multiplier UInt256
)
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY (asset, asset_source);

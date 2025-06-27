CREATE TABLE IF NOT EXISTS aave_ethereum.PriceLatestEventRawDenominator
(
    asset String,
    asset_source String,
    timestamp DateTime64(0),
    denominator UInt256
)
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY (asset, asset_source);

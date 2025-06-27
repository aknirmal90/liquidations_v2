CREATE TABLE IF NOT EXISTS aave_ethereum.PriceLatestTransactionRawMultiplier
(
    asset String,
    asset_source String,
    timestamp DateTime64(0),
    multiplier UInt256
)
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY asset;

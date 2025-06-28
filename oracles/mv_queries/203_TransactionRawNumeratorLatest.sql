CREATE TABLE IF NOT EXISTS aave_ethereum.PriceLatestTransactionRawNumerator
(
    asset String,
    asset_source String,
    name String,
    timestamp DateTime64(6),
    numerator UInt256
)
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY asset;

CREATE TABLE IF NOT EXISTS aave_ethereum.PriceLatestEventRawNumerator
(
    asset String,
    asset_source String,
    name String,
    blockTimestamp DateTime64(6),
    blockNumber UInt64,
    transactionHash String,
    type String,
    numerator UInt256
)
ENGINE = ReplacingMergeTree(blockTimestamp)
ORDER BY asset;

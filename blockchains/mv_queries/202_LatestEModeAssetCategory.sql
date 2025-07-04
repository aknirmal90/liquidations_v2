CREATE TABLE IF NOT EXISTS aave_ethereum.LatestEModeAssetCategoryChanged
(
    asset String,
    newCategoryId UInt8,
    transactionHash String,
    blockNumber UInt64,
    transactionIndex UInt32,
    logIndex UInt32,
    blockTimestamp DateTime64(6),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY asset;

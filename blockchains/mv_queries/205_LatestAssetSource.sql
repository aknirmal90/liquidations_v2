CREATE TABLE aave_ethereum.LatestAssetSourceUpdated
(
    asset String,
    source String,
    transactionHash String,
    blockNumber UInt64,
    transactionIndex UInt32,
    logIndex UInt32,
    blockTimestamp DateTime64(6),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY asset;

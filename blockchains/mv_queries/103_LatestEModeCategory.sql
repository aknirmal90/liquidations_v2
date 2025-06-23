CREATE TABLE aave_ethereum.LatestEModeCategoryAdded
(
    categoryId UInt8,
    ltv UInt16,
    liquidationThreshold UInt16,
    liquidationBonus UInt16,
    oracle String,
    label String,
    transactionHash String,
    blockNumber UInt64,
    transactionIndex UInt32,
    logIndex UInt32,
    blockTimestamp DateTime64(0),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY categoryId;

CREATE TABLE aave_ethereum.LatestCollateralConfigurationChanged
(
    asset String,
    ltv UInt16,
    liquidationThreshold UInt16,
    liquidationBonus UInt16,
    transactionHash String,
    blockNumber UInt64,
    transactionIndex UInt32,
    logIndex UInt32,
    blockTimestamp DateTime64(6),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY asset;

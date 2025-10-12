-- Create table for collateral status dictionary with ReplacingMergeTree engine
CREATE TABLE IF NOT EXISTS aave_ethereum.CollateralStatusDictionary
(
    user String,
    asset String,
    is_enabled_as_collateral Int8,
    blockNumber UInt64,
    transactionHash String,
    logIndex UInt64,
    blockTimestamp DateTime64,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (user, asset)
PRIMARY KEY (user, asset);

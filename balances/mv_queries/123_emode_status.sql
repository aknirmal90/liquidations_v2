-- Create table for eMode status dictionary with ReplacingMergeTree engine
CREATE TABLE IF NOT EXISTS aave_ethereum.EModeStatusDictionary
(
    user String,
    is_enabled_in_emode Int8,
    blockNumber UInt64,
    transactionHash String,
    logIndex UInt64,
    blockTimestamp DateTime64,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY user
PRIMARY KEY user;

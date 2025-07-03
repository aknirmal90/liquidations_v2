CREATE TABLE IF NOT EXISTS aave_ethereum.LatestNetworkBlockInfo
(
    network_name String,
    latest_block_number UInt64,
    latest_block_timestamp DateTime64(6),
    network_time_for_new_block UInt64
)
ENGINE = ReplacingMergeTree(latest_block_timestamp)
ORDER BY network_name;

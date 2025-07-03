CREATE TABLE IF NOT EXISTS aave_ethereum.NetworkBlockInfo
(
    network_id UInt8,
    latest_block_number UInt64,
    latest_block_timestamp DateTime64(6),
    network_time_for_new_block UInt64
)
ENGINE = MergeTree
ORDER BY (network_id, latest_block_timestamp);

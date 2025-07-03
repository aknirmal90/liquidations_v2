CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_latest_network_block_info
TO aave_ethereum.LatestNetworkBlockInfo
AS
SELECT
    network_name,
    latest_block_number,
    latest_block_timestamp,
    network_time_for_new_block
FROM aave_ethereum.NetworkBlockInfo;

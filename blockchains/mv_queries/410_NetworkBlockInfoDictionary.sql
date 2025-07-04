CREATE DICTIONARY IF NOT EXISTS aave_ethereum.NetworkBlockInfoDictionary
(
    network_id UInt8,
    latest_block_number UInt64,
    latest_block_timestamp DateTime64(6),
    network_time_for_new_block UInt64
)
PRIMARY KEY network_id
SOURCE(CLICKHOUSE(
    HOST 'localhost'
    PORT 9000
    USER 'clickhouse-user'
    PASSWORD 'clickhouse-password'
    TABLE 'LatestNetworkBlockInfo'
    DB 'aave_ethereum'
))
LIFETIME(MIN 3600 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED());

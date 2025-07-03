CREATE DICTIONARY IF NOT EXISTS aave_ethereum.MultiplierStatsDict
(
    asset String,
    asset_source String,
    name String,
    total_records UInt64,
    min_blockTimestamp DateTime64(6),
    max_blockTimestamp DateTime64(6),
    stddev_multiplier Float64,
    avg_time_bw_records Float64,
    std_growth_per_sec Int64
)
PRIMARY KEY asset, asset_source, name
SOURCE(CLICKHOUSE(
    HOST 'localhost'
    PORT 9000
    USER 'clickhouse-user'
    PASSWORD 'clickhouse-password'
    DB 'aave_ethereum'
    TABLE 'MultiplierStatistics'
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 300 MAX 600);

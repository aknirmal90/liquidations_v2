CREATE TABLE IF NOT EXISTS aave_ethereum.EventRawMaxCap
(
    asset String,
    asset_source String,
    name String,
    blockTimestamp DateTime64(6),
    blockNumber UInt64,
    max_cap UInt256,
    max_cap_type UInt8
)
ENGINE = Log;

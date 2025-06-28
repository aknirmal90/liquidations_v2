CREATE TABLE IF NOT EXISTS aave_ethereum.EventRawMaxCap
(
    asset String,
    asset_source String,
    name String,
    timestamp DateTime64(6),
    max_cap UInt256
)
ENGINE = Log;

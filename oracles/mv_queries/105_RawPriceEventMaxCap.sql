CREATE TABLE IF NOT EXISTS aave_ethereum.EventRawMaxCap
(
    asset String,
    asset_source String,
    max_cap UInt256,
    timestamp DateTime64(0)
)
ENGINE = Log;

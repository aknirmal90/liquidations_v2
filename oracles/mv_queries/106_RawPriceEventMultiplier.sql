CREATE TABLE IF NOT EXISTS aave_ethereum.EventRawMultiplier
(
    asset String,
    asset_source String,
    name String,
    timestamp DateTime64(6),
    multiplier UInt256
)
ENGINE = Log;

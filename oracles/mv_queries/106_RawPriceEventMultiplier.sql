CREATE TABLE IF NOT EXISTS aave_ethereum.EventRawMultiplier
(
    asset String,
    asset_source String,
    multiplier UInt256,
    timestamp DateTime64(0)
)
ENGINE = Log;

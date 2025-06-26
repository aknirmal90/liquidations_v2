CREATE TABLE IF NOT EXISTS aave_ethereum.EventRawNumerator
(
    asset String,
    asset_source String,
    numerator UInt256,
    timestamp DateTime64(0)
)
ENGINE = Log;

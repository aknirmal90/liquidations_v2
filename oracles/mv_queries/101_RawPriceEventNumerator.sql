CREATE TABLE IF NOT EXISTS aave_ethereum.EventRawNumerator
(
    asset String,
    asset_source String,
    name String,
    timestamp DateTime64(6),
    numerator UInt256
)
ENGINE = Log;

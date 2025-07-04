CREATE TABLE IF NOT EXISTS aave_ethereum.EventRawNumerator
(
    asset String,
    asset_source String,
    name String,
    blockTimestamp DateTime64(6),
    blockNumber UInt64,
    numerator UInt256
)
ENGINE = Log;

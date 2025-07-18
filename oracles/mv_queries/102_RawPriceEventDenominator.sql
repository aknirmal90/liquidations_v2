CREATE TABLE IF NOT EXISTS aave_ethereum.EventRawDenominator
(
    asset String,
    asset_source String,
    name String,
    blockTimestamp DateTime64(6),
    blockNumber UInt64,
    denominator UInt256
)
ENGINE = Log;

CREATE TABLE IF NOT EXISTS aave_ethereum.EventRawDenominator
(
    asset String,
    asset_source String,
    timestamp DateTime64(0),
    denominator UInt256
)
ENGINE = Log;

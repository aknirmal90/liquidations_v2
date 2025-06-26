CREATE TABLE IF NOT EXISTS aave_ethereum.TransactionRawDenominator
(
    asset String,
    asset_source String,
    denominator UInt256,
    timestamp DateTime64(0)
)
ENGINE = Log;

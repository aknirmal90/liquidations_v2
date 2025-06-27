CREATE TABLE IF NOT EXISTS aave_ethereum.TransactionRawNumerator
(
    asset String,
    asset_source String,
    timestamp DateTime64(0),
    numerator UInt256
)
ENGINE = Log;

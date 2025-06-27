CREATE TABLE IF NOT EXISTS aave_ethereum.TransactionRawMultiplier
(
    asset String,
    asset_source String,
    timestamp DateTime64(0),
    multiplier UInt256
)
ENGINE = Log;

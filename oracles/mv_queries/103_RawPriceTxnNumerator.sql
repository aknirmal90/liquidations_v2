CREATE TABLE IF NOT EXISTS aave_ethereum.TransactionRawNumerator
(
    asset String,
    asset_source String,
    name String,
    blockTimestamp DateTime64(6),
    blockNumber UInt64,
    transactionHash String,
    type String,
    numerator UInt256
)
ENGINE = Log;

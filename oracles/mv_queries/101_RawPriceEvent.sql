CREATE TABLE IF NOT EXISTS aave_ethereum.RawPriceEvent
(
    asset String,
    asset_source String,
    price UInt256,
    eventName String,
    contractAddress String,
    blockNumber UInt64,
    transactionHash String,
    transactionIndex UInt32,
    logIndex UInt32,
    blockTimestamp DateTime64(0)
)
ENGINE = Log;

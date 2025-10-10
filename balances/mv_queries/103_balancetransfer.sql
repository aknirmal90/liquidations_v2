CREATE TABLE IF NOT EXISTS aave_ethereum.BalanceTransfer
(
    `_from` String,
    `_to` String,
    `value` UInt256,
    `index` UInt256,
    `address` String,
    `blockNumber` UInt64,
    `transactionHash` String,
    `transactionIndex` UInt64,
    `logIndex` UInt64,
    `blockTimestamp` DateTime64,
    `type` String,
    `asset` String
)
ENGINE = Log;

CREATE TABLE IF NOT EXISTS aave_ethereum.TransactionTimingTracking
(
    txn_id String,
    asset_source AggregateFunction(any, String),
    unconfirmed_tx_ts AggregateFunction(max, UInt64),
    confirmed_tx_ts AggregateFunction(max, UInt64),
    mev_share_ts AggregateFunction(max, UInt64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (txn_id)
PRIMARY KEY (txn_id);

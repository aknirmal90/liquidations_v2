CREATE TABLE IF NOT EXISTS aave_ethereum.PriceMismatchCounts
(
    insert_timestamp DateTime64(6),
    historical_event_vs_rpc UInt32,
    historical_transaction_vs_rpc UInt32,
    predicted_transaction_vs_rpc UInt32,
    total_assets_verified UInt32,
    total_assets_different UInt32
)
ENGINE = MergeTree()
ORDER BY insert_timestamp
TTL toDateTime(insert_timestamp) + INTERVAL 7 DAY;

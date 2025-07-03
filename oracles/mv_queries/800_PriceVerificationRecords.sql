CREATE TABLE IF NOT EXISTS aave_ethereum.PriceVerificationRecords
(
    asset String,
    asset_source String,
    name String,
    type Enum('historical_event', 'historical_transaction', 'predicted_transaction'),
    blockTimestamp DateTime64(6),
    pct_error Float64
)
ENGINE = MergeTree()
ORDER BY (asset, asset_source, type, blockTimestamp)
TTL toDateTime(blockTimestamp) + INTERVAL 7 DAY;

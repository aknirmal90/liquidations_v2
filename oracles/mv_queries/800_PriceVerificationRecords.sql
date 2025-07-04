CREATE TABLE IF NOT EXISTS aave_ethereum.PriceVerificationRecords
(
    asset String,
    asset_source String,
    name String,
    type String,
    blockTimestamp DateTime64(6),
    pct_error Float64
)
ENGINE = MergeTree()
ORDER BY (asset, asset_source, type, blockTimestamp)
TTL toDateTime(blockTimestamp) + INTERVAL 7 DAY;

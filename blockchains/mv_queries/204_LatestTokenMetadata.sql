CREATE TABLE IF NOT EXISTS aave_ethereum.LatestTokenMetadata
(
    asset String,
    name String,
    symbol String,
    decimals_places String,
    decimals UInt8,
    blockTimestamp DateTime64(6)
)
ENGINE = ReplacingMergeTree(blockTimestamp)
ORDER BY asset;

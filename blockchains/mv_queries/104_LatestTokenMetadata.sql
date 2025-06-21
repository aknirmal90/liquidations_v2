CREATE TABLE aave_ethereum.LatestTokenMetadata
(
    asset String,
    name String,
    symbol String,
    decimals_places String,
    decimals UInt8,
    createdAt UInt64
)
ENGINE = ReplacingMergeTree(createdAt)
ORDER BY asset;

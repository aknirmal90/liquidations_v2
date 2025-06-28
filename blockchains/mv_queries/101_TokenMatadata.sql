CREATE TABLE aave_ethereum.TokenMetadata
(
    asset String,
    name String,
    symbol String,
    decimals_places UInt64,
    decimals UInt64,
    blockTimestamp DateTime64(6)
)
ENGINE = MergeTree
ORDER BY blockTimestamp;

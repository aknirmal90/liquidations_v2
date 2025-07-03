CREATE TABLE IF NOT EXISTS aave_ethereum.AssetSourceTokenMetadata
(
    asset_source String,
    decimals_places UInt64,
    decimals UInt64,
    blockTimestamp DateTime64(6)
)
ENGINE = MergeTree
ORDER BY blockTimestamp;

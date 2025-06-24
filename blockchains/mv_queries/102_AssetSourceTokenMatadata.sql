CREATE TABLE aave_ethereum.AssetSourceTokenMetadata
(
    asset_source String,
    decimals_places UInt64,
    decimals UInt64,
    blockTimestamp DateTime64(0)
)
ENGINE = MergeTree
ORDER BY blockTimestamp;

CREATE TABLE IF NOT EXISTS aave_ethereum.LatestAssetSourceTokenMetadata
(
    asset_source String,
    decimals_places UInt64,
    decimals UInt64,
    blockTimestamp DateTime64(6)
)
ENGINE = ReplacingMergeTree(blockTimestamp)
ORDER BY asset_source;

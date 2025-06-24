CREATE MATERIALIZED VIEW aave_ethereum.mv_latest_assetsource_token_metadata
TO aave_ethereum.LatestAssetSourceTokenMetadata
AS
SELECT
    asset_source,
    decimals_places,
    decimals,
    blockTimestamp
FROM aave_ethereum.AssetSourceTokenMetadata;

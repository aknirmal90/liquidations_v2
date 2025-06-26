CREATE VIEW IF NOT EXISTS aave_ethereum.USDPriceView
AS
SELECT
    aave_ethereum.LatestAssetSourceUpdated.asset AS asset,
    aave_ethereum.LatestRawPriceEvent.price AS price,
    aave_ethereum.LatestAssetSourceTokenMetadata.decimals_places AS decimals_places,
    aave_ethereum.LatestRawPriceEvent.price / pow(10, aave_ethereum.LatestAssetSourceTokenMetadata.decimals_places) AS price_usd
FROM aave_ethereum.LatestAssetSourceUpdated
INNER JOIN aave_ethereum.LatestRawPriceEvent
    ON aave_ethereum.LatestAssetSourceUpdated.asset = aave_ethereum.LatestRawPriceEvent.asset
INNER JOIN aave_ethereum.LatestAssetSourceTokenMetadata
    ON aave_ethereum.LatestAssetSourceUpdated.source = aave_ethereum.LatestAssetSourceTokenMetadata.asset_source;

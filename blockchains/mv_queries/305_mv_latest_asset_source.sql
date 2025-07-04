CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_latest_asset_source
TO aave_ethereum.LatestAssetSourceUpdated
AS
SELECT
    asset,
    source,
    transactionHash,
    blockNumber,
    transactionIndex,
    logIndex,
    blockTimestamp,
    (blockNumber * 1000000000 + transactionIndex * 10000 + logIndex) AS version
FROM aave_ethereum.AssetSourceUpdated;

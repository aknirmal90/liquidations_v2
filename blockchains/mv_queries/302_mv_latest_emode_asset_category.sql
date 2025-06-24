CREATE MATERIALIZED VIEW aave_ethereum.mv_latest_emode_asset_category
TO aave_ethereum.LatestEModeAssetCategoryChanged
AS
SELECT
    asset,
    newCategoryId,
    transactionHash,
    blockNumber,
    transactionIndex,
    logIndex,
    blockTimestamp,
    (blockNumber * 1000000000 + transactionIndex * 10000 + logIndex) AS version
FROM aave_ethereum.EModeAssetCategoryChanged;

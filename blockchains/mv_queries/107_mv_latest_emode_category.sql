CREATE MATERIALIZED VIEW aave_ethereum.mv_latest_emode_category
TO aave_ethereum.LatestEModeCategoryAdded
AS
SELECT
    categoryId,
    ltv,
    liquidationThreshold,
    liquidationBonus,
    oracle,
    label,
    transactionHash,
    blockNumber,
    transactionIndex,
    logIndex,
    (blockNumber * 1000000000 + transactionIndex * 10000 + logIndex) AS version
FROM aave_ethereum.EModeCategoryAdded;

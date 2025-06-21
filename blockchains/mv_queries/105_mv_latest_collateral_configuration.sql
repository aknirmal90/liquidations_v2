CREATE MATERIALIZED VIEW aave_ethereum.mv_latest_collateral_configuration
TO aave_ethereum.LatestCollateralConfigurationChanged
AS
SELECT
    asset,
    ltv,
    liquidationThreshold,
    liquidationBonus,
    transactionHash,
    blockNumber,
    transactionIndex,
    logIndex,
    (blockNumber * 1000000000 + transactionIndex * 10000 + logIndex) AS version
FROM aave_ethereum.CollateralConfigurationChanged;

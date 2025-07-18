CREATE OR REPLACE VIEW aave_ethereum.view_LatestAssetConfiguration AS
SELECT
    aave_ethereum.ReserveInitialized.asset AS asset,
    aave_ethereum.ReserveInitialized.aToken AS aToken,
    aave_ethereum.ReserveInitialized.stableDebtToken AS stableDebtToken,
    aave_ethereum.ReserveInitialized.variableDebtToken AS variableDebtToken,
    aave_ethereum.ReserveInitialized.interestRateStrategyAddress AS interestRateStrategyAddress,

    aave_ethereum.LatestCollateralConfigurationChanged.ltv AS collateralLTV,
    aave_ethereum.LatestCollateralConfigurationChanged.liquidationThreshold AS collateralLiquidationThreshold,
    aave_ethereum.LatestCollateralConfigurationChanged.liquidationBonus AS collateralLiquidationBonus,

    aave_ethereum.LatestEModeAssetCategoryChanged.newCategoryId AS eModeCategoryId,
    aave_ethereum.LatestEModeCategoryAdded.ltv AS eModeLTV,
    aave_ethereum.LatestEModeCategoryAdded.liquidationThreshold AS eModeLiquidationThreshold,
    aave_ethereum.LatestEModeCategoryAdded.liquidationBonus AS eModeLiquidationBonus,

    aave_ethereum.LatestTokenMetadata.name AS name,
    aave_ethereum.LatestTokenMetadata.symbol AS symbol,
    aave_ethereum.LatestTokenMetadata.decimals AS decimals,
    aave_ethereum.LatestTokenMetadata.decimals_places AS decimals_places

FROM aave_ethereum.ReserveInitialized
LEFT JOIN aave_ethereum.LatestCollateralConfigurationChanged
    ON aave_ethereum.ReserveInitialized.asset = aave_ethereum.LatestCollateralConfigurationChanged.asset
LEFT JOIN aave_ethereum.LatestEModeAssetCategoryChanged
    ON aave_ethereum.ReserveInitialized.asset = aave_ethereum.LatestEModeAssetCategoryChanged.asset
LEFT JOIN aave_ethereum.LatestEModeCategoryAdded
    ON aave_ethereum.LatestEModeAssetCategoryChanged.newCategoryId = aave_ethereum.LatestEModeCategoryAdded.categoryId
LEFT JOIN aave_ethereum.LatestTokenMetadata
    ON aave_ethereum.ReserveInitialized.asset = aave_ethereum.LatestTokenMetadata.asset;

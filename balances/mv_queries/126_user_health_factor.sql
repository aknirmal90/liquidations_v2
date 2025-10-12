-- Create view to calculate user health factors (optimized for speed)
CREATE OR REPLACE VIEW aave_ethereum.view_UserHealthFactor AS
SELECT
    lb.user AS user,
    lb.asset AS asset,

    -- Calculate effective collateral value (with liquidation threshold)
    CASE
        WHEN COALESCE(esd.is_enabled_in_emode, 0) = 1 AND ac.eModeLiquidationThreshold > 0 THEN
            toFloat64(sumMerge(lb.collateral_balance)) * lpe.historical_price_usd * ac.eModeLiquidationThreshold / 100.0
        WHEN COALESCE(csd.is_enabled_as_collateral, 0) = 1 AND ac.collateralLiquidationThreshold > 0 THEN
            toFloat64(sumMerge(lb.collateral_balance)) * lpe.historical_price_usd * ac.collateralLiquidationThreshold / 100.0
        ELSE 0
    END as effective_collateral_value_usd,
    maxMerge(lb.collateral_liquidityIndex) as collateral_liquidityIndex,

    -- Calculate debt value in USD
    toFloat64(sumMerge(lb.variable_debt_balance)) * lpe.historical_price_usd as debt_value_usd,
    maxMerge(lb.variable_debt_liquidityIndex) as variable_debt_liquidityIndex
FROM aave_ethereum.LatestBalances lb
LEFT JOIN aave_ethereum.view_LatestAssetConfiguration ac ON lb.asset = ac.asset
LEFT JOIN aave_ethereum.LatestPriceEvent lpe ON lb.asset = lpe.asset
LEFT JOIN aave_ethereum.CollateralStatusDictionary csd ON lb.user = csd.user AND lb.asset = csd.asset
LEFT JOIN aave_ethereum.EModeStatusDictionary esd ON lb.user = esd.user
GROUP BY
    lb.user,
    lb.asset,
    ac.collateralLiquidationThreshold,
    ac.eModeLiquidationThreshold,
    lpe.historical_price_usd,
    esd.is_enabled_in_emode,
    csd.is_enabled_as_collateral;

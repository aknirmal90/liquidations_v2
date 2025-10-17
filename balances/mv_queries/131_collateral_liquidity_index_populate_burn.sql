-- Materialized view to populate CollateralLiquidityIndex from Burn events
-- Only inserts collateral-type events with blockNumber as version
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_collateral_liquidity_index_from_burn
TO aave_ethereum.CollateralLiquidityIndex
AS SELECT
    asset,
    index as liquidityIndex,
    index as version
FROM aave_ethereum.Burn
WHERE type = 'Collateral';

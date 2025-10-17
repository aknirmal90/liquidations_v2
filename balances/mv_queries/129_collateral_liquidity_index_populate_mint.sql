-- Materialized view to populate CollateralLiquidityIndex from Mint events
-- Only inserts collateral-type events with blockNumber as version
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_collateral_liquidity_index_from_mint
TO aave_ethereum.CollateralLiquidityIndex
AS SELECT
    asset,
    index as liquidityIndex,
    index as version
FROM aave_ethereum.Mint
WHERE type = 'Collateral';

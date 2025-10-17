-- View to get the latest collateral liquidity index per asset
-- Uses FINAL to get the latest version from ReplacingMergeTree
CREATE OR REPLACE VIEW aave_ethereum.view_collateral_liquidity_index AS
SELECT
    asset,
    liquidityIndex
FROM aave_ethereum.CollateralLiquidityIndex
FINAL;

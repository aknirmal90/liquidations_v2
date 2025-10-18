-- View to get the latest collateral liquidity index per asset
-- Uses FINAL to get the latest version from ReplacingMergeTree
CREATE VIEW IF NOT EXISTS aave_ethereum.view_collateral_liquidity_index AS
SELECT
    asset,
    liquidityIndex
FROM aave_ethereum.CollateralLiquidityIndex
FINAL;

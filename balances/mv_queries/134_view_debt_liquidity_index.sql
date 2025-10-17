-- View to get the latest debt liquidity index per asset
-- Uses FINAL to get the latest version from ReplacingMergeTree
CREATE OR REPLACE VIEW aave_ethereum.view_debt_liquidity_index AS
SELECT
    asset,
    liquidityIndex
FROM aave_ethereum.DebtLiquidityIndex
FINAL;

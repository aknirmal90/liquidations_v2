-- View to get the latest debt liquidity index per asset
-- Uses FINAL to get the latest version from ReplacingMergeTree
CREATE VIEW IF NOT EXISTS aave_ethereum.view_debt_liquidity_index AS
SELECT
    asset,
    liquidityIndex,
    updated_at_block,
    interest_rate
FROM aave_ethereum.DebtLiquidityIndex
FINAL;

-- New LatestBalances table that stores SCALED balances instead of underlying balances
-- Scaled balances are computed using ray math: scaled = floor((underlying * RAY) / liquidityIndex)
-- To get current underlying balance: underlying = floor((scaled * currentLiquidityIndex) / RAY)
-- where RAY = 1e27
CREATE TABLE IF NOT EXISTS aave_ethereum.LatestBalances_v2
(
    user String,
    asset String,
    -- Scaled balances in ray units (no index needed - scales with global index automatically)
    collateral_scaled_balance AggregateFunction(sum, Int256),
    variable_debt_scaled_balance AggregateFunction(sum, Int256)
)
ENGINE = AggregatingMergeTree()
ORDER BY (user, asset);

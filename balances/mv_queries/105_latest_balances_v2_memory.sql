-- In-memory table for fast queries on latest balances
-- Populated periodically from LatestBalances_v2
-- Only includes rows where collateral or debt balance > 0
CREATE TABLE IF NOT EXISTS aave_ethereum.LatestBalances_v2_Memory
(
    user String,
    asset String,
    collateral_scaled_balance Int256,
    variable_debt_scaled_balance Int256,
    updated_at DateTime64
)
ENGINE = Memory;

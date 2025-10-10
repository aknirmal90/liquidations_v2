CREATE TABLE IF NOT EXISTS aave_ethereum.LatestBalances
(
    user String,
    asset String,
    is_enabled_as_collateral AggregateFunction(sum, Int8),
    collateral_liquidityIndex AggregateFunction(max, UInt256),
    collateral_balance AggregateFunction(sum, Int256),
    stable_debt_liquidityIndex AggregateFunction(max, UInt256),
    stable_debt_balance AggregateFunction(sum, Int256),
    variable_debt_liquidityIndex AggregateFunction(max, UInt256),
    variable_debt_balance AggregateFunction(sum, Int256)
)
ENGINE = AggregatingMergeTree()
ORDER BY (user, asset);

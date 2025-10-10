-- MV Query: Materialized View for Burn event (StableDebt)
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_stabledebt_balances_burn TO aave_ethereum.LatestBalances
AS SELECT
    from as user,
    asset as asset,
    sumState(toInt8(0)) as is_enabled_as_collateral,
    maxState(toUInt256(0)) as collateral_liquidityIndex,
    sumState(toInt256(0)) as collateral_balance,
    maxState(index) as stable_debt_liquidityIndex,
    sumState(toInt256(-1 * value + balanceIncrease)) as stable_debt_balance,
    maxState(toUInt256(0)) as variable_debt_liquidityIndex,
    sumState(toInt256(0)) as variable_debt_balance
FROM aave_ethereum.Burn
WHERE type = 'StableDebt'
GROUP BY from, asset;

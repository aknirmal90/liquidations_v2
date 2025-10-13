-- MV Query: Materialized View for Burn event (VariableDebt)
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_variabledebt_balances_burn TO aave_ethereum.LatestBalances
AS SELECT
    from as user,
    asset as asset,
    maxState(toUInt256(0)) as collateral_liquidityIndex,
    sumState(toInt256(0)) as collateral_balance,
    maxState(index) as variable_debt_liquidityIndex,
    sumState(toInt256(-1 * (value + balanceIncrease))) as variable_debt_balance
FROM aave_ethereum.Burn
WHERE type = 'VariableDebt'
GROUP BY from, asset;

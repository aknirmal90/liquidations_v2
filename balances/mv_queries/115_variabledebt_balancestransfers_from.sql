-- MV Query: Materialized View for _from addresses in BalanceTransfer (VariableDebt)
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_variabledebt_balances_from TO aave_ethereum.LatestBalances
AS SELECT
    _from as user,
    asset as asset,
    maxState(toUInt256(0)) as collateral_liquidityIndex,
    sumState(toInt256(0)) as collateral_balance,
    maxState(index) as variable_debt_liquidityIndex,
    sumState(toInt256(-1 * value)) as variable_debt_balance
FROM aave_ethereum.BalanceTransfer
WHERE type = 'VariableDebt'
GROUP BY _from, asset;

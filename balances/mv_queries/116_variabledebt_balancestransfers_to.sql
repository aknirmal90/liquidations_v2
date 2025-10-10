-- MV Query: Materialized View for _to addresses in BalanceTransfer (VariableDebt)
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_variabledebt_balances_to TO aave_ethereum.LatestBalances
AS SELECT
    _to as user,
    asset as asset,
    sumState(toInt8(0)) as is_enabled_as_collateral,
    maxState(toUInt256(0)) as collateral_liquidityIndex,
    sumState(toInt256(0)) as collateral_balance,
    maxState(toUInt256(0)) as stable_debt_liquidityIndex,
    sumState(toInt256(0)) as stable_debt_balance,
    maxState(index) as variable_debt_liquidityIndex,
    sumState(toInt256(value)) as variable_debt_balance
FROM aave_ethereum.BalanceTransfer
WHERE type = 'VariableDebt'
GROUP BY _to, asset;

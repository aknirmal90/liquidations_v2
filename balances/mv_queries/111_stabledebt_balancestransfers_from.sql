-- MV Query: Materialized View for _from addresses in BalanceTransfer (StableDebt)
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_stabledebt_balances_from TO aave_ethereum.LatestBalances
AS SELECT
    _from as user,
    asset as asset,
    sumState(toInt8(0)) as is_enabled_as_collateral,
    maxState(toUInt256(0)) as collateral_liquidityIndex,
    sumState(toInt256(0)) as collateral_balance,
    maxState(index) as stable_debt_liquidityIndex,
    sumState(toInt256(-1 * value)) as stable_debt_balance,
    maxState(toUInt256(0)) as variable_debt_liquidityIndex,
    sumState(toInt256(0)) as variable_debt_balance
FROM aave_ethereum.BalanceTransfer
WHERE type = 'StableDebt'
GROUP BY _from, asset;

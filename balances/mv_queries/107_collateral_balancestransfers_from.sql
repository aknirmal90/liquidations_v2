-- MV Query 1: Materialized View for _from addresses in BalanceTransfer
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_balances_from TO aave_ethereum.LatestBalances
AS SELECT
    _from as user,
    asset as asset,
    maxState(index) as collateral_liquidityIndex,
    sumState(toInt256(-1 * value)) as collateral_balance,
    maxState(toUInt256(0)) as variable_debt_liquidityIndex,
    sumState(toInt256(0)) as variable_debt_balance
FROM aave_ethereum.BalanceTransfer
WHERE type = 'Collateral'
GROUP BY _from, asset;

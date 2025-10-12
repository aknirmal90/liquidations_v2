-- MV Query 2: Materialized View for _to addresses in BalanceTransfer
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_balances_to TO aave_ethereum.LatestBalances
AS SELECT
    _to as user,
    asset as asset,
    maxState(index) as collateral_liquidityIndex,
    sumState(toInt256(value)) as collateral_balance,
    maxState(toUInt256(0)) as variable_debt_liquidityIndex,
    sumState(toInt256(0)) as variable_debt_balance
FROM aave_ethereum.BalanceTransfer
WHERE type = 'Collateral'
GROUP BY _to, asset;

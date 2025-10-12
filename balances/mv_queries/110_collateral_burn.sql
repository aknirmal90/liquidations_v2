-- MV Query: Materialized View for Burn event
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_balances_burn TO aave_ethereum.LatestBalances
AS SELECT
    from as user,
    asset as asset,
    maxState(index) as collateral_liquidityIndex,
    sumState(toInt256(-1 * value + balanceIncrease)) as collateral_balance,
    maxState(toUInt256(0)) as variable_debt_liquidityIndex,
    sumState(toInt256(0)) as variable_debt_balance
FROM aave_ethereum.Burn
WHERE type = 'Collateral'
GROUP BY from, asset;

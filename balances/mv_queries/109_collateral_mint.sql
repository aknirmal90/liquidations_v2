-- MV Query: Materialized View for Mint event
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_balances_mint TO aave_ethereum.LatestBalances
AS SELECT
    onBehalfOf as user,
    asset as asset,
    maxState(index) as collateral_liquidityIndex,
    sumState(toInt256(value - balanceIncrease)) as collateral_balance,
    maxState(toUInt256(0)) as variable_debt_liquidityIndex,
    sumState(toInt256(0)) as variable_debt_balance
FROM aave_ethereum.Mint
WHERE type = 'Collateral'
GROUP BY onBehalfOf, asset;

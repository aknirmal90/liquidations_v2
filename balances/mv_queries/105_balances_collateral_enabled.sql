-- MV Query 3: Materialized View for ReserveUsedAsCollateralEnabled
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_balances_collateral_enabled TO aave_ethereum.LatestBalances
AS SELECT
    user,
    reserve as asset,
    maxState(toUInt256(0)) as collateral_liquidityIndex,
    sumState(toInt256(0)) as collateral_balance,
    maxState(toUInt256(0)) as variable_debt_liquidityIndex,
    sumState(toInt256(0)) as variable_debt_balance
FROM aave_ethereum.ReserveUsedAsCollateralEnabled
GROUP BY user, reserve;

-- MV Query 4: Materialized View for ReserveUsedAsCollateralDisabled
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_balances_collateral_disabled TO aave_ethereum.LatestBalances
AS SELECT
    user,
    reserve as asset,
    sumState(toInt8(-1)) as is_enabled_as_collateral,
    maxState(toUInt256(0)) as collateral_liquidityIndex,
    sumState(toInt256(0)) as collateral_balance,
    maxState(toUInt256(0)) as stable_debt_liquidityIndex,
    sumState(toInt256(0)) as stable_debt_balance,
    maxState(toUInt256(0)) as variable_debt_liquidityIndex,
    sumState(toInt256(0)) as variable_debt_balance
FROM aave_ethereum.ReserveUsedAsCollateralDisabled
GROUP BY user, reserve;

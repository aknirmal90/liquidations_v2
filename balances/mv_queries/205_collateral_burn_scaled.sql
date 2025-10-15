-- MV Query: Materialized View for Burn event (Collateral) with correct ray math scaling
-- Converts underlying delta (value + balanceIncrease) to scaled units (negative)
-- Formula: scaled_delta = -floor((value + balanceIncrease) * RAY / index)
-- where RAY = 1e27
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_balances_burn_scaled TO aave_ethereum.LatestBalances_v2
AS SELECT
    from as user,
    asset as asset,
    -- Convert to scaled (negative): -floor((value + balanceIncrease) * RAY / index)
    sumState(toInt256(
        -1 * floor((toDecimal256(value + balanceIncrease, 0) * toDecimal256('1000000000000000000000000000', 0)) / toDecimal256(index, 0))
    )) as collateral_scaled_balance,
    sumState(toInt256(0)) as variable_debt_scaled_balance
FROM aave_ethereum.Burn
WHERE type = 'Collateral' AND index > 0
GROUP BY from, asset;

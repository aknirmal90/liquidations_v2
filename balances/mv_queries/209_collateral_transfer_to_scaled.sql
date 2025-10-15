-- MV Query: Materialized View for BalanceTransfer _to (Collateral) with correct ray math scaling
-- Converts transfer amount to scaled units using block-level liquidity index
-- Formula: scaled_delta = floor(value * RAY / block_index)
-- where RAY = 1e27
CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_balances_transfer_to_scaled TO aave_ethereum.LatestBalances_v2
AS SELECT
    bt._to as user,
    bt.asset as asset,
    -- Convert to scaled: floor(value * RAY / block_index)
    sumState(toInt256(
        floor((toDecimal256(bt.value, 0) * toDecimal256('1000000000000000000000000000', 0)) / toDecimal256(bli.max_collateral_liquidityIndex, 0))
    )) as collateral_scaled_balance,
    sumState(toInt256(0)) as variable_debt_scaled_balance
FROM aave_ethereum.BalanceTransfer bt
LEFT JOIN aave_ethereum.BlockLiquidityIndex bli
    ON bt.asset = bli.asset AND bt.blockNumber = bli.block_number
WHERE bt.type = 'Collateral' AND bli.max_collateral_liquidityIndex > 0
GROUP BY bt._to, bt.asset;

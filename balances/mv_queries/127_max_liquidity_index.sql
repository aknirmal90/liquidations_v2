-- Create table for maximum liquidity index values at asset level using AggregatingMergeTree
CREATE TABLE IF NOT EXISTS aave_ethereum.MaxLiquidityIndex
(
    asset String,
    max_collateral_liquidityIndex SimpleAggregateFunction(max, UInt256),
    max_variable_debt_liquidityIndex SimpleAggregateFunction(max, UInt256),
    last_updated DateTime64
)
ENGINE = AggregatingMergeTree()
ORDER BY asset
PRIMARY KEY asset;

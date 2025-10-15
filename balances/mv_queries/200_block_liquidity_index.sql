-- Create table for block-level maximum liquidity index values
-- This table is needed to scale transfer events, which don't include index in their event data
CREATE TABLE IF NOT EXISTS aave_ethereum.BlockLiquidityIndex
(
    asset String,
    block_number UInt64,
    max_collateral_liquidityIndex SimpleAggregateFunction(max, UInt256),
    max_variable_debt_liquidityIndex SimpleAggregateFunction(max, UInt256)
)
ENGINE = AggregatingMergeTree()
ORDER BY (asset, block_number)
PRIMARY KEY (asset, block_number);

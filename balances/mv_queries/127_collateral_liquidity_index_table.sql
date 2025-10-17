-- Table for collateral liquidity index using ReplacingMergeTree
-- Stores the latest liquidity index per asset with version tracking
CREATE TABLE IF NOT EXISTS aave_ethereum.CollateralLiquidityIndex
(
    asset String,
    liquidityIndex UInt256,
    version UInt256  -- Use blockNumber or timestamp as version
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (asset)
PRIMARY KEY asset;

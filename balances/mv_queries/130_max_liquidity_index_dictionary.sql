-- Create dictionary for MaxLiquidityIndex for fast lookups by asset address
-- This allows using dictGet() functions to retrieve max liquidity indices
CREATE DICTIONARY IF NOT EXISTS aave_ethereum.dict_max_liquidity_index
(
    asset String,
    max_collateral_liquidityIndex UInt256,
    max_variable_debt_liquidityIndex UInt256,
    last_updated DateTime64
)
PRIMARY KEY asset
SOURCE(CLICKHOUSE(
    TABLE 'MaxLiquidityIndexFinal'
    DB 'aave_ethereum'
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 0 MAX 60);

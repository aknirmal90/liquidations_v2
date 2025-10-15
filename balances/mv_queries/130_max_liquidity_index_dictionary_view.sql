-- Create in-memory dictionary view for MaxLiquidityIndex
CREATE DICTIONARY IF NOT EXISTS aave_ethereum.dict_max_liquidity_index
(
    asset String,
    max_collateral_liquidityIndex UInt256,
    max_variable_debt_liquidityIndex UInt256,
    last_updated DateTime64
)
PRIMARY KEY asset
SOURCE(CLICKHOUSE(
    HOST 'localhost'
    PORT 9000
    USER 'clickhouse-user'
    PASSWORD 'clickhouse-password'
    DB 'aave_ethereum'
    TABLE 'MaxLiquidityIndex'
))
LAYOUT(HASHED())
LIFETIME(MIN 15 MAX 15);

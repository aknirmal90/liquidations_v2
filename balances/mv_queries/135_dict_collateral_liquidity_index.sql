-- Dictionary for fast lookups of collateral liquidity index by asset
CREATE DICTIONARY IF NOT EXISTS aave_ethereum.dict_collateral_liquidity_index
(
    asset String,
    liquidityIndex UInt256
)
PRIMARY KEY asset
SOURCE(CLICKHOUSE(
    HOST 'localhost'
    PORT 9000
    USER 'clickhouse-user'
    PASSWORD 'clickhouse-password'
    DB 'aave_ethereum'
    TABLE 'view_collateral_liquidity_index'
))
LAYOUT(HASHED())
LIFETIME(MIN 0 MAX 60);

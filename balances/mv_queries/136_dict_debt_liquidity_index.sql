-- Dictionary for fast lookups of debt liquidity index by asset
-- Uses COMPLEX_KEY_HASHED for String key type
CREATE DICTIONARY IF NOT EXISTS aave_ethereum.dict_debt_liquidity_index
(
    asset String,
    liquidityIndex UInt256,
    updated_at_block UInt64,
    interest_rate UInt256
)
PRIMARY KEY asset
SOURCE(CLICKHOUSE(
    HOST 'localhost'
    PORT 9000
    USER 'clickhouse-user'
    PASSWORD 'clickhouse-password'
    DB 'aave_ethereum'
    TABLE 'view_debt_liquidity_index'
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 1 MAX 1);

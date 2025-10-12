-- Create in-memory dictionary view for CollateralStatusDictionary
CREATE DICTIONARY IF NOT EXISTS aave_ethereum.dict_collateral_status
(
    user String,
    asset String,
    is_enabled_as_collateral Int8
)
PRIMARY KEY user, asset
SOURCE(CLICKHOUSE(
    HOST 'localhost'
    PORT 9000
    USER 'clickhouse-user'
    PASSWORD 'clickhouse-password'
    DB 'aave_ethereum'
    TABLE 'CollateralStatusDictionary'
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 15 MAX 15);

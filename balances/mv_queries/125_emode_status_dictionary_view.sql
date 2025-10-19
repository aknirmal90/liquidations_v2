-- Create in-memory dictionary view for EModeStatusDictionary
CREATE DICTIONARY IF NOT EXISTS aave_ethereum.dict_emode_status
(
    user String,
    is_enabled_in_emode Int8
)
PRIMARY KEY user
SOURCE(CLICKHOUSE(
    HOST 'localhost'
    PORT 9000
    USER 'clickhouse-user'
    PASSWORD 'clickhouse-password'
    DB 'aave_ethereum'
    TABLE 'EModeStatusDictionary'
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 1 MAX 1);

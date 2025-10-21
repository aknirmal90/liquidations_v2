-- Create dictionary for fast swap path lookups
-- Loaded from SwapPaths Log table
-- Auto-reloads every second to stay in sync with the table
--
-- USAGE:
--   SELECT dictGet('aave_ethereum.dict_swap_paths', 'path',
--                  ('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
--                   '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'))
--
--   Returns: '0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8'

CREATE DICTIONARY IF NOT EXISTS aave_ethereum.dict_swap_paths
(
    token_in String,
    token_out String,
    path String
)
PRIMARY KEY token_in, token_out
SOURCE(CLICKHOUSE(
    HOST 'localhost'
    PORT 9000
    USER 'default'
    PASSWORD ''
    DB 'aave_ethereum'
    TABLE 'SwapPaths'
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(1);  -- Reload every 1 second

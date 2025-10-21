-- Create Log table for optimal swap paths between priority assets
-- Stores the best swap path (pool addresses) for each token pair
-- Updated every second by Celery task using atomic table swap
-- Priority Assets: WETH, USDT, USDC, WBTC
--
-- This table is used as the source for the SwapPaths dictionary
-- which provides fast lookups for liquidation execution

CREATE TABLE IF NOT EXISTS aave_ethereum.SwapPaths
(
    token_in String,
    token_out String,
    path String COMMENT 'Comma-separated pool addresses: pool1,pool2 (or just pool1 for 1-hop)',
    updated_at DateTime DEFAULT now()
)
ENGINE = Log;

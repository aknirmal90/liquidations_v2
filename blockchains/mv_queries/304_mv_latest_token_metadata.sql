CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_latest_token_metadata
TO aave_ethereum.LatestTokenMetadata
AS
SELECT
    asset,
    name,
    symbol,
    decimals_places,
    decimals,
    blockTimestamp
FROM aave_ethereum.TokenMetadata;

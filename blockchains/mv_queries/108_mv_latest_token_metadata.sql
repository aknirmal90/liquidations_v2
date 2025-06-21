CREATE MATERIALIZED VIEW aave_ethereum.mv_latest_token_metadata
TO aave_ethereum.LatestTokenMetadata
AS
SELECT
    asset,
    name,
    symbol,
    decimals_places,
    decimals,
    createdAt
FROM aave_ethereum.TokenMetadata;

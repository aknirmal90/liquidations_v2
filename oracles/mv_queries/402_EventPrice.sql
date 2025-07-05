CREATE VIEW IF NOT EXISTS aave_ethereum.LatestPriceEvent AS
SELECT
    asset,
    asset_source,
    name,
    blockTimestamp,
    blockNumber,
    multiplier_blockTimestamp,
    multiplier_blockNumber,
    numerator,
    denominator,
    multiplier,
    max_cap,
    max_cap_type,
    decimals_places,
    max_cap_uint256,
    multiplier_cap,
    CAST(LEAST(multiplier, multiplier_cap) * LEAST(max_cap_uint256, historical_price) AS UInt256) AS historical_price,
    CAST(LEAST(multiplier, multiplier_cap) * LEAST(max_cap_uint256, historical_price) / CAST(decimals_places AS Float64) AS UInt256) AS historical_price_usd
FROM aave_ethereum.LatestPriceEventBase

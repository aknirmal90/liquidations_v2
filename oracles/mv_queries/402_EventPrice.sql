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
    IF(
        max_cap_type IN (0,1),
        CAST(LEAST(max_cap_uint256, CAST(historical_price_max_cap_precomputed AS Float64)) AS UInt256),
        CAST(LEAST(CAST(multiplier AS Float64), multiplier_cap) * CAST(numerator AS Float64) / CAST(denominator AS Float64) AS UInt256)
    ) AS historical_price,
    IF(
        max_cap_type IN (0,1),
        CAST(LEAST(max_cap_uint256, CAST(historical_price_max_cap_precomputed AS Float64)) AS Float64),
        CAST(LEAST(CAST(multiplier AS Float64), multiplier_cap) * CAST(numerator AS Float64) / CAST(denominator AS Float64) AS Float64)
    ) / CAST(decimals_places AS Float64) AS historical_price_usd
FROM aave_ethereum.LatestPriceEventBase

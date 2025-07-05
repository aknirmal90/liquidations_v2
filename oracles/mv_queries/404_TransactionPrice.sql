CREATE VIEW IF NOT EXISTS aave_ethereum.LatestPriceTransaction AS
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
    CAST(LEAST(CAST(multiplier AS Float64) * (1 + multiplier_growth_to_current_block / CAST(multiplier AS Float64)), CAST(multiplier_cap AS Float64)) * LEAST(CAST(max_cap_uint256 AS Float64), CAST(historical_price AS Float64)) AS UInt256) AS historical_price,
    CAST(LEAST(CAST(multiplier AS Float64) * (1 + multiplier_growth_to_next_block / CAST(multiplier AS Float64)), CAST(multiplier_cap AS Float64)) * LEAST(CAST(max_cap_uint256 AS Float64), CAST(historical_price AS Float64)) / CAST(decimals_places AS Float64) AS UInt256) AS predicted_price
FROM aave_ethereum.LatestPriceTransactionBase

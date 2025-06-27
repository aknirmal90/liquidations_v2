CREATE VIEW IF NOT EXISTS aave_ethereum.LatestPriceEvent AS
SELECT
    asset,
    asset_source,
    timestamp,
    numerator,
    denominator,
    multiplier,
    max_cap,
    decimals_places,
    -- Calculate base price: numerator / denominator * multiplier
    CAST(
        CASE
        WHEN max_cap > 0 THEN
            -- Apply max cap when it exists
            LEAST(
                (CAST(numerator AS Float64) / CAST(denominator AS Float64) * CAST(multiplier AS Float64)),
                CASE
                    WHEN multiplier = 1 THEN CAST(max_cap AS Float64)
                    ELSE (CAST(numerator AS Float64) / CAST(denominator AS Float64) * CAST(max_cap AS Float64))
                END
            )
        ELSE
            -- No max cap applied
            CAST(numerator AS Float64) / CAST(denominator AS Float64) * CAST(multiplier AS Float64)
        END AS Int64
    ) AS historical_price,
    -- Calculate USD price by dividing by decimals_places
    CAST(
        CASE
        WHEN max_cap > 0 THEN
            -- Apply max cap when it exists
            LEAST(
                (CAST(numerator AS Float64) / CAST(denominator AS Float64) * CAST(multiplier AS Float64)),
                CASE
                    WHEN multiplier = 1 THEN CAST(max_cap AS Float64)
                    ELSE (CAST(numerator AS Float64) / CAST(denominator AS Float64) * CAST(max_cap AS Float64))
                END
            )
        ELSE
            -- No max cap applied
            CAST(numerator AS Float64) / CAST(denominator AS Float64) * CAST(multiplier AS Float64)
        END AS Int64
    ) / CAST(decimals_places AS Float64) AS historical_price_usd
FROM aave_ethereum.LatestPriceEventBase

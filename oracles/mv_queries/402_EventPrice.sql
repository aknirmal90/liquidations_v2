CREATE VIEW IF NOT EXISTS aave_ethereum.LatestPriceEvent AS
SELECT
    asset,
    asset_source,
    name,
    timestamp,
    numerator,
    denominator,
    multiplier,
    max_cap,
    decimals_places,
    -- Calculate base price: numerator / denominator * multiplier
    CASE
    WHEN max_cap > 0 THEN
        -- Apply max cap when it exists
        LEAST(
            historical_price,
            CASE
                WHEN multiplier = 1 THEN CAST(max_cap AS UInt256)
                ELSE CAST(CAST(numerator AS Float64) / CAST(denominator AS Float64) * CAST(max_cap AS Float64) AS UInt256)
            END
        )
    ELSE
        -- No max cap applied
        historical_price
    END AS historical_price,
    CAST(
        CASE
        WHEN max_cap > 0 THEN
            -- Apply max cap when it exists
            LEAST(
                historical_price,
                CASE
                    WHEN multiplier = 1 THEN CAST(max_cap AS UInt256)
                    ELSE CAST(CAST(numerator AS Float64) / CAST(denominator AS Float64) * CAST(max_cap AS Float64) AS UInt256)
                END
            )
        ELSE
            -- No max cap applied
            historical_price
        END AS Float64
    ) / CAST(decimals_places AS Float64) AS historical_price_usd
FROM aave_ethereum.LatestPriceEventBase

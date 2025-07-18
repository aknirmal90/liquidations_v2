CREATE VIEW IF NOT EXISTS aave_ethereum.LatestPriceEventBase AS
SELECT
    aave_ethereum.PriceLatestEventRawNumerator.asset AS asset,
    aave_ethereum.PriceLatestEventRawNumerator.asset_source AS asset_source,
    aave_ethereum.PriceLatestEventRawNumerator.name AS name,
    aave_ethereum.PriceLatestEventRawNumerator.blockTimestamp AS blockTimestamp,
    aave_ethereum.PriceLatestEventRawNumerator.blockNumber AS blockNumber,
    aave_ethereum.PriceLatestEventRawMultiplier.blockTimestamp AS multiplier_blockTimestamp,
    aave_ethereum.PriceLatestEventRawMultiplier.blockNumber AS multiplier_blockNumber,
    aave_ethereum.PriceLatestEventRawNumerator.numerator AS numerator,
    aave_ethereum.PriceLatestEventRawDenominator.denominator AS denominator,
    aave_ethereum.PriceLatestEventRawMultiplier.multiplier AS multiplier,
    aave_ethereum.PriceLatestEventRawMaxCap.max_cap AS max_cap,
    aave_ethereum.PriceLatestEventRawMaxCap.max_cap_type AS max_cap_type,
    CAST(pow(10, aave_ethereum.LatestAssetSourceTokenMetadata.decimals_places) AS UInt256) AS decimals_places,
    IF(
        aave_ethereum.PriceLatestEventRawMaxCap.max_cap_type = 1,
        CAST(aave_ethereum.PriceLatestEventRawMaxCap.max_cap AS Float64),
        CAST('1.7976931348623157e+308' AS Float64)
    ) AS max_cap_uint256,
    IF(
        aave_ethereum.PriceLatestEventRawMaxCap.max_cap_type = 2,
        CAST(aave_ethereum.PriceLatestEventRawMaxCap.max_cap AS Float64),
        CAST(1 AS Float64)
    ) AS multiplier_cap,
    CAST((
        CAST(aave_ethereum.PriceLatestEventRawNumerator.numerator AS Float64)
        / CAST(aave_ethereum.PriceLatestEventRawDenominator.denominator AS Float64)
        * CAST(aave_ethereum.PriceLatestEventRawMultiplier.multiplier AS Float64)
    ) AS UInt256) AS historical_price_max_cap_precomputed
FROM aave_ethereum.PriceLatestEventRawNumerator
INNER JOIN aave_ethereum.PriceLatestEventRawDenominator ON aave_ethereum.PriceLatestEventRawNumerator.asset = aave_ethereum.PriceLatestEventRawDenominator.asset
INNER JOIN aave_ethereum.PriceLatestEventRawMultiplier ON aave_ethereum.PriceLatestEventRawNumerator.asset = aave_ethereum.PriceLatestEventRawMultiplier.asset
INNER JOIN aave_ethereum.PriceLatestEventRawMaxCap ON aave_ethereum.PriceLatestEventRawNumerator.asset = aave_ethereum.PriceLatestEventRawMaxCap.asset
INNER JOIN aave_ethereum.LatestAssetSourceTokenMetadata
    ON aave_ethereum.PriceLatestEventRawNumerator.asset_source = aave_ethereum.LatestAssetSourceTokenMetadata.asset_source;

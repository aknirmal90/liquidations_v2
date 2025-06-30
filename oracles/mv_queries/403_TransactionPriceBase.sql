CREATE VIEW IF NOT EXISTS aave_ethereum.LatestPriceTransactionBase AS
SELECT
    aave_ethereum.PriceLatestTransactionRawNumerator.asset AS asset,
    aave_ethereum.PriceLatestTransactionRawNumerator.asset_source AS asset_source,
    aave_ethereum.PriceLatestTransactionRawNumerator.name AS name,
    aave_ethereum.PriceLatestTransactionRawNumerator.timestamp AS timestamp,
    aave_ethereum.PriceLatestTransactionRawNumerator.numerator AS numerator,
    aave_ethereum.PriceLatestEventRawDenominator.denominator AS denominator,
    aave_ethereum.PriceLatestTransactionRawMultiplier.multiplier AS multiplier,
    aave_ethereum.PriceLatestEventRawMaxCap.max_cap AS max_cap,
    CAST(pow(10, aave_ethereum.LatestAssetSourceTokenMetadata.decimals_places) AS UInt256) AS decimals_places,
    CAST((
        CAST(aave_ethereum.PriceLatestTransactionRawNumerator.numerator AS Float64)
        / CAST(aave_ethereum.PriceLatestEventRawDenominator.denominator AS Float64)
        * CAST(aave_ethereum.PriceLatestTransactionRawMultiplier.multiplier AS Float64)
    ) AS UInt256) AS historical_price
FROM aave_ethereum.PriceLatestTransactionRawNumerator
INNER JOIN aave_ethereum.PriceLatestEventRawDenominator ON aave_ethereum.PriceLatestTransactionRawNumerator.asset = aave_ethereum.PriceLatestEventRawDenominator.asset
INNER JOIN aave_ethereum.PriceLatestTransactionRawMultiplier ON aave_ethereum.PriceLatestTransactionRawNumerator.asset = aave_ethereum.PriceLatestTransactionRawMultiplier.asset
INNER JOIN aave_ethereum.PriceLatestEventRawMaxCap ON aave_ethereum.PriceLatestTransactionRawNumerator.asset = aave_ethereum.PriceLatestEventRawMaxCap.asset
INNER JOIN aave_ethereum.LatestAssetSourceTokenMetadata
    ON aave_ethereum.PriceLatestTransactionRawNumerator.asset_source = aave_ethereum.LatestAssetSourceTokenMetadata.asset_source;

CREATE VIEW IF NOT EXISTS aave_ethereum.LatestPriceTransactionBase AS
SELECT
    aave_ethereum.PriceLatestTransactionRawNumerator.asset,
    aave_ethereum.PriceLatestTransactionRawNumerator.asset_source,
    aave_ethereum.PriceLatestTransactionRawNumerator.timestamp,
    aave_ethereum.PriceLatestTransactionRawNumerator.numerator,
    aave_ethereum.PriceLatestEventRawDenominator.denominator,
    aave_ethereum.PriceLatestEventRawMultiplier.multiplier,
    aave_ethereum.PriceLatestEventRawMaxCap.max_cap,
    IF(
        aave_ethereum.PriceLatestEventRawMaxCap.max_cap > 0,
        CAST(aave_ethereum.PriceLatestTransactionRawNumerator.numerator AS Float64) / CAST(aave_ethereum.PriceLatestEventRawDenominator.denominator AS Float64) * CAST(aave_ethereum.PriceLatestEventRawMultiplier.multiplier AS Float64),
        CASE WHEN CAST(aave_ethereum.PriceLatestTransactionRawNumerator.numerator AS Float64) / CAST(aave_ethereum.PriceLatestEventRawDenominator.denominator AS Float64) * CAST(aave_ethereum.PriceLatestEventRawMultiplier.multiplier AS Float64) > CAST(aave_ethereum.PriceLatestEventRawMaxCap.max_cap AS Float64) THEN CAST(aave_ethereum.PriceLatestEventRawMaxCap.max_cap AS Float64) ELSE CAST(aave_ethereum.PriceLatestTransactionRawNumerator.numerator AS Float64) / CAST(aave_ethereum.PriceLatestEventRawDenominator.denominator AS Float64) * CAST(aave_ethereum.PriceLatestEventRawMultiplier.multiplier AS Float64) END
    ) AS historical_price,
    IF(
        aave_ethereum.PriceLatestEventRawMaxCap.max_cap > 0,
        CAST(aave_ethereum.PriceLatestTransactionRawNumerator.numerator AS Float64) / CAST(aave_ethereum.PriceLatestEventRawDenominator.denominator AS Float64) * CAST(aave_ethereum.PriceLatestEventRawMultiplier.multiplier AS Float64),
        CASE WHEN CAST(aave_ethereum.PriceLatestTransactionRawNumerator.numerator AS Float64) / CAST(aave_ethereum.PriceLatestEventRawDenominator.denominator AS Float64) * CAST(aave_ethereum.PriceLatestEventRawMultiplier.multiplier AS Float64) > CAST(aave_ethereum.PriceLatestEventRawMaxCap.max_cap AS Float64) THEN CAST(aave_ethereum.PriceLatestEventRawMaxCap.max_cap AS Float64) ELSE CAST(aave_ethereum.PriceLatestTransactionRawNumerator.numerator AS Float64) / CAST(aave_ethereum.PriceLatestEventRawDenominator.denominator AS Float64) * CAST(aave_ethereum.PriceLatestEventRawMultiplier.multiplier AS Float64) END
    ) / pow(10, aave_ethereum.LatestAssetSourceTokenMetadata.decimals_places) AS historical_price_usd
FROM aave_ethereum.PriceLatestTransactionRawNumerator
INNER JOIN aave_ethereum.PriceLatestEventRawDenominator ON aave_ethereum.PriceLatestTransactionRawNumerator.asset = aave_ethereum.PriceLatestEventRawDenominator.asset
INNER JOIN aave_ethereum.PriceLatestEventRawMultiplier ON aave_ethereum.PriceLatestTransactionRawNumerator.asset = aave_ethereum.PriceLatestEventRawMultiplier.asset
INNER JOIN aave_ethereum.PriceLatestEventRawMaxCap ON aave_ethereum.PriceLatestTransactionRawNumerator.asset = aave_ethereum.PriceLatestEventRawMaxCap.asset
INNER JOIN aave_ethereum.LatestAssetSourceTokenMetadata
    ON aave_ethereum.PriceLatestTransactionRawNumerator.asset_source = aave_ethereum.LatestAssetSourceTokenMetadata.asset_source;

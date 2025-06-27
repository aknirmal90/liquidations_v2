CREATE VIEW IF NOT EXISTS aave_ethereum.LatestPriceTransaction AS
SELECT
    aave_ethereum.PriceLatestTransactionRawNumerator.asset,
    aave_ethereum.PriceLatestTransactionRawNumerator.asset_source,
    aave_ethereum.PriceLatestTransactionRawNumerator.timestamp,
    aave_ethereum.PriceLatestTransactionRawNumerator.numerator,
    aave_ethereum.PriceLatestTransactionRawDenominator.denominator,
    aave_ethereum.PriceLatestTransactionRawMultiplier.multiplier,
    aave_ethereum.PriceLatestEventRawMaxCap.max_cap,
    IF(
        aave_ethereum.PriceLatestEventRawMaxCap.max_cap > 0,
        CAST(aave_ethereum.PriceLatestTransactionRawNumerator.numerator AS Float64) / CAST(aave_ethereum.PriceLatestTransactionRawDenominator.denominator AS Float64) * CAST(aave_ethereum.PriceLatestTransactionRawMultiplier.multiplier AS Float64),
        CASE WHEN CAST(aave_ethereum.PriceLatestTransactionRawNumerator.numerator AS Float64) / CAST(aave_ethereum.PriceLatestTransactionRawDenominator.denominator AS Float64) * CAST(aave_ethereum.PriceLatestTransactionRawMultiplier.multiplier AS Float64) > CAST(aave_ethereum.PriceLatestEventRawMaxCap.max_cap AS Float64) THEN CAST(aave_ethereum.PriceLatestEventRawMaxCap.max_cap AS Float64) ELSE CAST(aave_ethereum.PriceLatestTransactionRawNumerator.numerator AS Float64) / CAST(aave_ethereum.PriceLatestTransactionRawDenominator.denominator AS Float64) * CAST(aave_ethereum.PriceLatestTransactionRawMultiplier.multiplier AS Float64) END
    ) AS historical_price,
    IF(
        aave_ethereum.PriceLatestEventRawMaxCap.max_cap > 0,
        CAST(aave_ethereum.PriceLatestTransactionRawNumerator.numerator AS Float64) / CAST(aave_ethereum.PriceLatestTransactionRawDenominator.denominator AS Float64) * CAST(aave_ethereum.PriceLatestTransactionRawMultiplier.multiplier AS Float64),
        CASE WHEN CAST(aave_ethereum.PriceLatestTransactionRawNumerator.numerator AS Float64) / CAST(aave_ethereum.PriceLatestTransactionRawDenominator.denominator AS Float64) * CAST(aave_ethereum.PriceLatestTransactionRawMultiplier.multiplier AS Float64) > CAST(aave_ethereum.PriceLatestEventRawMaxCap.max_cap AS Float64) THEN CAST(aave_ethereum.PriceLatestEventRawMaxCap.max_cap AS Float64) ELSE CAST(aave_ethereum.PriceLatestTransactionRawNumerator.numerator AS Float64) / CAST(aave_ethereum.PriceLatestTransactionRawDenominator.denominator AS Float64) * CAST(aave_ethereum.PriceLatestTransactionRawMultiplier.multiplier AS Float64) END
    ) / pow(10, aave_ethereum.LatestAssetSourceTokenMetadata.decimals_places) AS historical_price_usd
FROM aave_ethereum.PriceLatestTransactionRawNumerator
INNER JOIN aave_ethereum.PriceLatestTransactionRawDenominator ON aave_ethereum.PriceLatestTransactionRawNumerator.asset = aave_ethereum.PriceLatestTransactionRawDenominator.asset
INNER JOIN aave_ethereum.PriceLatestTransactionRawMultiplier ON aave_ethereum.PriceLatestTransactionRawNumerator.asset = aave_ethereum.PriceLatestTransactionRawMultiplier.asset
INNER JOIN aave_ethereum.PriceLatestEventRawMaxCap ON aave_ethereum.PriceLatestTransactionRawNumerator.asset = aave_ethereum.PriceLatestEventRawMaxCap.asset
INNER JOIN aave_ethereum.LatestAssetSourceTokenMetadata
    ON aave_ethereum.PriceLatestTransactionRawNumerator.asset_source = aave_ethereum.LatestAssetSourceTokenMetadata.asset_source;

CREATE VIEW IF NOT EXISTS aave_ethereum.LatestPriceTransactionBase AS
SELECT
    aave_ethereum.PriceLatestTransactionRawNumerator.asset AS asset,
    aave_ethereum.PriceLatestTransactionRawNumerator.asset_source AS asset_source,
    aave_ethereum.PriceLatestTransactionRawNumerator.name AS name,
    aave_ethereum.PriceLatestTransactionRawNumerator.blockTimestamp AS blockTimestamp,
    aave_ethereum.PriceLatestTransactionRawNumerator.blockNumber AS blockNumber,
    aave_ethereum.PriceLatestTransactionRawMultiplier.blockTimestamp AS multiplier_blockTimestamp,
    aave_ethereum.PriceLatestTransactionRawMultiplier.blockNumber AS multiplier_blockNumber,
    aave_ethereum.PriceLatestTransactionRawNumerator.numerator AS numerator,
    aave_ethereum.PriceLatestEventRawDenominator.denominator AS denominator,
    aave_ethereum.PriceLatestTransactionRawMultiplier.multiplier AS multiplier,
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
    CAST(
        dictGet(
            'aave_ethereum.MultiplierStatsDict',
            'std_growth_per_sec',
            (aave_ethereum.PriceLatestTransactionRawMultiplier.asset, aave_ethereum.PriceLatestTransactionRawMultiplier.asset_source, aave_ethereum.PriceLatestTransactionRawMultiplier.name)
        ) *
        (dictGet('aave_ethereum.NetworkBlockInfoDictionary', 'latest_block_number', 1) - aave_ethereum.PriceLatestTransactionRawMultiplier.blockNumber) *
        dictGet('aave_ethereum.NetworkBlockInfoDictionary', 'network_time_for_new_block', 1) AS Float64
    ) AS multiplier_growth_to_current_block,
    CAST(
        dictGet(
            'aave_ethereum.MultiplierStatsDict',
            'std_growth_per_sec',
            (aave_ethereum.PriceLatestTransactionRawMultiplier.asset, aave_ethereum.PriceLatestTransactionRawMultiplier.asset_source, aave_ethereum.PriceLatestTransactionRawMultiplier.name)
        ) *
        (dictGet('aave_ethereum.NetworkBlockInfoDictionary', 'latest_block_number', 1) + 1 - aave_ethereum.PriceLatestTransactionRawMultiplier.blockNumber) *
        dictGet('aave_ethereum.NetworkBlockInfoDictionary', 'network_time_for_new_block', 1) AS Float64
    ) AS multiplier_growth_to_next_block,
    CAST((
        CAST(aave_ethereum.PriceLatestTransactionRawNumerator.numerator AS Float64)
        / CAST(aave_ethereum.PriceLatestEventRawDenominator.denominator AS Float64)
        * CAST(aave_ethereum.PriceLatestTransactionRawMultiplier.multiplier AS Float64)
    ) AS UInt256) AS historical_price_max_cap_precomputed
FROM aave_ethereum.PriceLatestTransactionRawNumerator
INNER JOIN aave_ethereum.PriceLatestEventRawDenominator ON aave_ethereum.PriceLatestTransactionRawNumerator.asset = aave_ethereum.PriceLatestEventRawDenominator.asset
INNER JOIN aave_ethereum.PriceLatestTransactionRawMultiplier ON aave_ethereum.PriceLatestTransactionRawNumerator.asset = aave_ethereum.PriceLatestTransactionRawMultiplier.asset
INNER JOIN aave_ethereum.PriceLatestEventRawMaxCap ON aave_ethereum.PriceLatestTransactionRawNumerator.asset = aave_ethereum.PriceLatestEventRawMaxCap.asset
INNER JOIN aave_ethereum.LatestAssetSourceTokenMetadata
    ON aave_ethereum.PriceLatestTransactionRawNumerator.asset_source = aave_ethereum.LatestAssetSourceTokenMetadata.asset_source;

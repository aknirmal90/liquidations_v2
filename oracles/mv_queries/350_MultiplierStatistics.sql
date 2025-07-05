CREATE VIEW IF NOT EXISTS aave_ethereum.MultiplierStatistics AS
SELECT
    asset,
    asset_source,
    name,
    count() AS total_records,
    min(blockTimestamp) AS min_blockTimestamp,
    max(blockTimestamp) AS max_blockTimestamp,
    stddevPop(toFloat64(multiplier)) AS stddev_multiplier,
    (toUnixTimestamp(max(blockTimestamp)) - toUnixTimestamp(min(blockTimestamp))) / count() AS avg_time_bw_records,
    CASE
        WHEN toUnixTimestamp(max(blockTimestamp)) - toUnixTimestamp(min(blockTimestamp)) > 0 THEN
            CAST((max(toFloat64(multiplier)) - min(toFloat64(multiplier))) /
            (toUnixTimestamp(max(blockTimestamp)) - toUnixTimestamp(min(blockTimestamp))) AS Float64)
        ELSE CAST(0 AS Float64)
    END AS std_growth_per_sec
FROM aave_ethereum.TransactionRawMultiplier
WHERE blockTimestamp >= now() - INTERVAL 7 DAY
GROUP BY
    asset,
    asset_source,
    name
ORDER BY
    asset,
    asset_source,
    name;

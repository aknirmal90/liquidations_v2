CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_event_raw_denominator_latest
TO aave_ethereum.PriceLatestEventRawDenominator
AS
SELECT
    asset,
    asset_source,
    timestamp,
    denominator
FROM aave_ethereum.EventRawDenominator;

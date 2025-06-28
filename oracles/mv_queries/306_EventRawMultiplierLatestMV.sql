CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_event_raw_multiplier_latest
TO aave_ethereum.PriceLatestEventRawMultiplier
AS
SELECT
    asset,
    asset_source,
    name,
    timestamp,
    multiplier
FROM aave_ethereum.EventRawMultiplier;

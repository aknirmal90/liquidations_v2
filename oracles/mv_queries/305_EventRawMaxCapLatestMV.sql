CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_event_raw_max_cap_latest
TO aave_ethereum.PriceLatestEventRawMaxCap
AS
SELECT
    asset,
    asset_source,
    name,
    timestamp,
    max_cap
FROM aave_ethereum.EventRawMaxCap;

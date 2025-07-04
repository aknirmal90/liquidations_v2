CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_transaction_raw_multiplier_latest
TO aave_ethereum.PriceLatestTransactionRawMultiplier
AS
SELECT
    asset,
    asset_source,
    name,
    blockTimestamp,
    blockNumber,
    multiplier
FROM aave_ethereum.TransactionRawMultiplier;

CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_transaction_raw_denominator_latest
TO aave_ethereum.PriceLatestTransactionRawDenominator
AS
SELECT
    asset,
    asset_source,
    timestamp,
    denominator
FROM aave_ethereum.TransactionRawDenominator;

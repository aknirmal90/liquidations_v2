CREATE MATERIALIZED VIEW IF NOT EXISTS aave_ethereum.mv_transaction_raw_numerator_latest
TO aave_ethereum.PriceLatestTransactionRawNumerator
AS
SELECT
    asset,
    asset_source,
    name,
    blockTimestamp,
    blockNumber,
    transactionHash,
    type,
    numerator
FROM aave_ethereum.TransactionRawNumerator;

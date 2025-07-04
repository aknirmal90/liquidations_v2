-- Insert into TransactionRawNumerator table
INSERT INTO aave_ethereum.TransactionRawNumerator
(
    asset,
    asset_source,
    name,
    blockTimestamp,
    blockNumber,
    numerator
)
VALUES
(
    '0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f',
    '0xd110cac5d8682a3b045d5524a9903e031d70fccd',
    'GhoOracle',
    now(),
    1,
    100000000
);

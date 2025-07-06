-- Insert into EventRawNumerator table
INSERT INTO aave_ethereum.EventRawNumerator
(
    asset,
    asset_source,
    name,
    blockTimestamp,
    blockNumber,
    transactionHash,
    type,
    numerator
)
VALUES
(
    '0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f',
    '0xd110cac5d8682a3b045d5524a9903e031d70fccd',
    'GhoOracle',
    now(),
    1,
    '0x0000000000000000000000000000000000000000000000000000000000000000',
    'event',
    100000000
);

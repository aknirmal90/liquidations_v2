-- Insert into TransactionRawDenominator table
INSERT INTO aave_ethereum.TransactionRawDenominator
(
    asset,
    asset_source,
    name,
    timestamp,
    denominator
)
VALUES
(
    '0xd110cac5d8682a3b045d5524a9903e031d70fccd',
    '0xd110cac5d8682a3b045d5524a9903e031d70fccd',
    'GhoOracle',
    now(),
    1
);

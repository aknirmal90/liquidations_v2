-- Insert into EventRawMaxCap table
INSERT INTO aave_ethereum.EventRawMaxCap
(
    asset,
    asset_source,
    name,
    blockTimestamp,
    blockNumber,
    max_cap,
    max_cap_type
)
VALUES
(
    '0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f',
    '0xd110cac5d8682a3b045d5524a9903e031d70fccd',
    'GhoOracle',
    now(),
    1,
    0,
    0  -- NO_CAP for GHO
);

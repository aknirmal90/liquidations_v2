INSERT INTO aave_ethereum.RawPriceEvent
(
    asset,
    asset_source,
    price,
    eventName,
    contractAddress,
    blockNumber,
    transactionHash,
    transactionIndex,
    logIndex,
    blockTimestamp
)
VALUES
(
    '0xd110cac5d8682a3b045d5524a9903e031d70fccd',
    '0xd110cac5d8682a3b045d5524a9903e031d70fccd',
    100000000,
    'Constant',
    '0xd110cac5d8682a3b045d5524a9903e031d70fccd',
    0,
    '0xd110cac5d8682a3b045d5524a9903e031d70fccd',
    0,
    0,
    toDateTime64(0, 0)
);

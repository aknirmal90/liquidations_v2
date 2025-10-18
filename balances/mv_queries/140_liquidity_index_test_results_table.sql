-- Table for storing collateral liquidity index test results
CREATE TABLE IF NOT EXISTS aave_ethereum.LiquidityIndexTestResults
(
    test_timestamp DateTime64(3),
    total_assets UInt32,
    matching_records UInt32,
    mismatched_records UInt32,
    match_percentage Float64,
    avg_difference_bps Float64,
    max_difference_bps Float64,
    test_duration_seconds Float64,
    test_status String,
    error_message String DEFAULT '',
    mismatches_detail String DEFAULT ''
)
ENGINE = MergeTree()
ORDER BY test_timestamp
SETTINGS index_granularity = 8192;

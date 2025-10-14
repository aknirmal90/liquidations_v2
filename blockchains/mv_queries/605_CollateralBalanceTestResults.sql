-- Table to store collateral balance validation test results
-- Compares ClickHouse computed balances against RPC data from DataProvider contract

CREATE TABLE IF NOT EXISTS aave_ethereum.CollateralBalanceTestResults
(
    test_run_id UUID DEFAULT generateUUIDv4(),
    test_timestamp DateTime64(6) DEFAULT now64(),
    batch_offset UInt32 DEFAULT 0,
    total_user_assets UInt32,
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

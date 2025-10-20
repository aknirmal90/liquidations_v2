-- Table for storing liquidation candidates test results
-- Tests validate that collateral_balance and debt_to_cover are accurate
-- by comparing against on-chain aToken and variableDebtToken balances
CREATE TABLE IF NOT EXISTS aave_ethereum.LiquidationCandidatesTestResults
(
    test_timestamp DateTime64(3),
    total_candidates UInt32,
    matching_records UInt32,
    mismatched_records UInt32,
    match_percentage Float64,
    avg_collateral_difference_bps Float64,
    max_collateral_difference_bps Float64,
    avg_debt_difference_bps Float64,
    max_debt_difference_bps Float64,
    test_duration_seconds Float64,
    test_status String,
    error_message String DEFAULT '',
    mismatches_detail String DEFAULT ''
)
ENGINE = MergeTree()
ORDER BY test_timestamp
SETTINGS index_granularity = 8192;

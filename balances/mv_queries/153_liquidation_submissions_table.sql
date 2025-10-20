-- Create Log table for MEV liquidation submissions tracking
-- This table tracks all liquidation transaction submissions to MEV builders
--
-- Each row represents one submission attempt to a specific builder
-- Multiple rows can exist for the same liquidation (one per builder)
--
-- Uses Log engine for fast, append-only inserts and permanent storage

CREATE TABLE IF NOT EXISTS aave_ethereum.LiquidationSubmissions
(
    builder_name String,
    user String,
    collateral_asset String,
    debt_asset String,
    expected_profit Float64,
    nonce UInt64,
    target_block UInt64,
    bundle_hash String,
    tx_hash String,
    submission_success UInt8,
    error_message String DEFAULT '',
    submitted_at DateTime DEFAULT now()
)
ENGINE = Log;

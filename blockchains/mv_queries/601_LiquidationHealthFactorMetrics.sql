-- ClickHouse table for storing liquidation health factor metrics
-- This table stores precalculated health factor data for liquidation events
-- at various transaction/block points for analysis and optimization

CREATE TABLE IF NOT EXISTS aave_ethereum.LiquidationHealthFactorMetrics (
    -- Transaction identification
    transaction_hash String,
    transaction_index UInt32,
    log_index UInt64,
    block_number UInt64,
    block_timestamp DateTime,

    -- User and liquidation details
    user_address String,
    liquidator_address String,
    collateral_asset String,
    debt_asset String,
    liquidated_collateral_amount UInt256,
    debt_to_cover UInt256,

    -- Health factor calculations at different points
    health_factor_at_transaction Nullable(Decimal(38, 18)),        -- HF at the exact transaction
    health_factor_at_previous_tx Nullable(Decimal(38, 18)),        -- HF at transaction index - 1
    health_factor_at_block_start Nullable(Decimal(38, 18)),        -- HF at start of block (tx index 0)
    health_factor_at_previous_block Nullable(Decimal(38, 18)),     -- HF at previous block
    health_factor_at_two_blocks_prior Nullable(Decimal(38, 18)),   -- HF at 2 blocks prior

    -- Metadata
    processed_at DateTime DEFAULT now(),
    calculation_errors String DEFAULT ''  -- Store any errors encountered during calculation
)
ENGINE = MergeTree()
ORDER BY (transaction_hash, block_number, transaction_index, log_index)
PARTITION BY toYYYYMM(block_timestamp)
TTL block_timestamp + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

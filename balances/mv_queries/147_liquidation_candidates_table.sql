-- Create Memory table for liquidation candidates
-- This table stores profitable liquidation opportunities for Aave positions
-- Candidates are users with health factors between 0.9 and 1.25
-- and significant positions (>$10,000 in both collateral and debt)
-- This table is populated by a Celery task that runs periodically

CREATE TABLE IF NOT EXISTS aave_ethereum.LiquidationCandidates_Memory
(
    user String,
    collateral_asset String,
    debt_asset String,
    debt_to_cover Float64,
    profit Float64,
    health_factor Float64,
    effective_collateral Float64,
    effective_debt Float64,
    collateral_balance Float64,
    debt_balance Float64,
    liquidation_bonus UInt256,
    collateral_price Float64,
    debt_price Float64,
    collateral_decimals UInt256,
    debt_decimals UInt256,
    is_priority_debt UInt8,
    is_priority_collateral UInt8,
    updated_at DateTime DEFAULT now()
)
ENGINE = Memory;

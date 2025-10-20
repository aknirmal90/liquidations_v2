-- Create Log table for liquidation detections
-- This table stores users who have health factor > 1 on current view (146)
-- but < 1 when using predicted transaction prices for updated assets
--
-- This table is appended to after each transaction numerator update
-- to track users who may become liquidatable based on predicted prices
--
-- Uses Log engine for fast inserts and permanent storage

CREATE TABLE IF NOT EXISTS aave_ethereum.LiquidationDetections
(
    user String,
    collateral_asset String,
    debt_asset String,
    current_health_factor Float64,
    predicted_health_factor Float64,
    debt_to_cover Float64,
    profit Float64,
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
    updated_assets Array(String),
    detected_at DateTime DEFAULT now()
)
ENGINE = Log;

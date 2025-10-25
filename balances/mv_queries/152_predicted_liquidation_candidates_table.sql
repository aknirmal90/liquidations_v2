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
    current_health_factor Decimal256(18),
    predicted_health_factor Decimal256(18),
    debt_to_cover Decimal256(0),
    profit Decimal256(18),
    effective_collateral Decimal256(18),
    effective_debt Decimal256(18),
    collateral_balance Decimal256(0),
    debt_balance Decimal256(0),
    liquidation_bonus UInt256,
    collateral_price Decimal256(18),
    debt_price Decimal256(18),
    collateral_decimals UInt256,
    debt_decimals UInt256,
    updated_assets Array(String),
    detected_at DateTime DEFAULT now()
)
ENGINE = Log;

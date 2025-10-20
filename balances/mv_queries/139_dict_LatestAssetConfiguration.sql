-- Create in-memory dictionary view for LatestAssetConfiguration
CREATE DICTIONARY IF NOT EXISTS aave_ethereum.dict_latest_asset_configuration
(
    asset String,
    aToken String,
    variableDebtToken String,
    interestRateStrategyAddress String,
    collateralLTV UInt256,
    collateralLiquidationThreshold UInt256,
    collateralLiquidationBonus UInt256,
    eModeCategoryId UInt8,
    eModeLTV UInt256,
    eModeLiquidationThreshold UInt256,
    eModeLiquidationBonus UInt256,
    name String,
    symbol String,
    decimals UInt8,
    decimals_places UInt256,
    historical_event_price Float64,
    historical_event_price_usd Float64,
    predicted_transaction_price UInt256,
    predicted_transaction_price_usd Float64,
    max_collateral_liquidityIndex UInt256,
    max_variable_debt_liquidityIndex UInt256
)
PRIMARY KEY asset
SOURCE(CLICKHOUSE(
    HOST 'localhost'
    PORT 9000
    USER 'clickhouse-user'
    PASSWORD 'clickhouse-password'
    DB 'aave_ethereum'
    TABLE 'view_LatestAssetConfiguration'
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 1 MAX 1);

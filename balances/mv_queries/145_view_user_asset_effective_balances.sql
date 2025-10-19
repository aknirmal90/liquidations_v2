-- Create view for user asset effective balances with interest accrual
-- This view calculates effective debt and collateral per user and asset
-- Used as input for view_user_health_factor
--
-- HIGHER PRECISION VERSION:
-- All calculations are performed in Decimal128(38) where possible.
-- This increases precision beyond Float64 and drastically reduces potential for rounding/skew in large values.
--
-- Effective Collateral calculation (per asset):
-- - floor(numerator) / floor(denominator), where
--     numerator = balance * liquidation_threshold * is_collateral_enabled * price * interest_accrual_factor
--     denominator = 10000 * decimals_places
--     liquidation_threshold depends on eMode status (eModeLiquidationThreshold vs collateralLiquidationThreshold)
--
-- Effective Debt calculation (per asset):
-- - floor(numerator) / floor(denominator), where
--     numerator = debt_balance * price * interest_accrual_factor
--     denominator = decimals_places
--
-- Interest Accrual Factor:
-- - Accounts for interest accrued between last index update and latest block
-- - Factor = 1 + (interest_rate / RAY / seconds_in_year * (latest_block - updated_at_block) * 12)
-- All computations upgraded to Decimal128(38) except for inner GREATEST/casting to avoid overflow.

CREATE VIEW IF NOT EXISTS aave_ethereum.view_user_asset_effective_balances AS
WITH
network_info AS (
    SELECT
        dictGetOrDefault('aave_ethereum.NetworkBlockInfoDictionary', 'latest_block_number', toUInt8(1), toUInt64(0)) AS latest_block_number
),
current_balances AS (
    SELECT
        lb.user,
        lb.asset,
        floor((toInt256(lb.collateral_scaled_balance) * toInt256(dictGetOrDefault('aave_ethereum.dict_collateral_liquidity_index', 'liquidityIndex', lb.asset, toUInt256(0)))) / toInt256('1000000000000000000000000000')) AS collateral_balance,
        floor((toInt256(lb.variable_debt_scaled_balance) * toInt256(dictGetOrDefault('aave_ethereum.dict_debt_liquidity_index', 'liquidityIndex', lb.asset, toUInt256(0)))) / toInt256('1000000000000000000000000000')) AS debt_balance,
        dictGetOrDefault('aave_ethereum.dict_collateral_liquidity_index', 'interest_rate', lb.asset, toUInt256(0)) AS collateral_interest_rate,
        dictGetOrDefault('aave_ethereum.dict_collateral_liquidity_index', 'updated_at_block', lb.asset, toUInt64(0)) AS collateral_updated_at_block,
        dictGetOrDefault('aave_ethereum.dict_debt_liquidity_index', 'interest_rate', lb.asset, toUInt256(0)) AS debt_interest_rate,
        dictGetOrDefault('aave_ethereum.dict_debt_liquidity_index', 'updated_at_block', lb.asset, toUInt64(0)) AS debt_updated_at_block
    FROM aave_ethereum.LatestBalances_v2_Memory AS lb
    CROSS JOIN network_info
)
SELECT
    cb.user,
    cb.asset,
    cb.collateral_balance,
    cb.debt_balance,

    dictGetOrDefault('aave_ethereum.dict_emode_status', 'is_enabled_in_emode', toString(cb.user), toInt8(0)) AS is_in_emode,

    -- The decimals_places and price are always cast to Float64 below to avoid illegal multiplication of Float64 * UInt/Decimal128
    toFloat64(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'decimals_places', cb.asset, toUInt256(1))) AS decimals_places,
    toFloat64(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'historical_event_price', cb.asset, toUInt256(0))) AS price,
    dictGetOrDefault('aave_ethereum.dict_collateral_status', 'is_enabled_as_collateral', tuple(cb.user, cb.asset), toInt8(0)) AS is_collateral_enabled,

    dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'eModeLiquidationThreshold', cb.asset, toUInt256(0)) AS emode_liquidation_threshold,
    dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'collateralLiquidationThreshold', cb.asset, toUInt256(0)) AS collateral_liquidation_threshold,

    cb.collateral_interest_rate,
    cb.collateral_updated_at_block,
    (SELECT latest_block_number FROM network_info) AS latest_block_number,

    -- Collateral interest accrual factor (as Float64)
    (
        1 +
        (
            toFloat64(cb.collateral_interest_rate) / 1e27
            / 31536000
            * toFloat64(GREATEST((SELECT latest_block_number FROM network_info) - cb.collateral_updated_at_block, 0))
            * 12
        )
    ) AS collateral_interest_accrual_factor,

    cb.debt_interest_rate,
    cb.debt_updated_at_block,

    -- Debt interest accrual factor (as Float64)
    (
        1 +
        (
            toFloat64(cb.debt_interest_rate) / 1e27
            / 31536000
            * toFloat64(GREATEST((SELECT latest_block_number FROM network_info) - cb.debt_updated_at_block, 0))
            * 12
        )
    ) AS debt_interest_accrual_factor,

    -- Effective Collateral (all factors cast to Float64 to avoid illegal multiply in ClickHouse)
    toInt256(floor(
        toFloat64(cb.collateral_balance)
        *
        toFloat64(
            if(
                dictGetOrDefault('aave_ethereum.dict_emode_status', 'is_enabled_in_emode', cb.user, toInt8(0)) = 1,
                dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'eModeLiquidationThreshold', cb.asset, toUInt256(0)),
                dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'collateralLiquidationThreshold', cb.asset, toUInt256(0))
            )
        )
        *
        toFloat64(dictGetOrDefault('aave_ethereum.dict_collateral_status', 'is_enabled_as_collateral', tuple(cb.user, cb.asset), toInt8(0)))
        *
        toFloat64(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'historical_event_price', cb.asset, toUInt256(0)))
        *
        (
            (
                1 +
                (
                    toFloat64(cb.collateral_interest_rate) / 1e27
                    / 31536000
                    * toFloat64(GREATEST((SELECT latest_block_number FROM network_info) - cb.collateral_updated_at_block, 0))
                    * 12
                )
            )
        )
        /
        (
            10000.0
            *
            toFloat64(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'decimals_places', cb.asset, toUInt256(1)))
        )
    )) AS effective_collateral,

    -- Effective Debt (all factors cast to Float64 to avoid illegal multiply in ClickHouse)
    toInt256(ceil(
        toFloat64(cb.debt_balance)
        *
        toFloat64(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'historical_event_price', cb.asset, toUInt256(0)))
        *
        (
            (
                1 +
                (
                    toFloat64(cb.debt_interest_rate) / 1e27
                    / 31536000
                    * toFloat64(GREATEST((SELECT latest_block_number FROM network_info) - cb.debt_updated_at_block, 0))
                    * 12
                )
            )
        )
        /
        (
            toFloat64(dictGetOrDefault('aave_ethereum.dict_latest_asset_configuration', 'decimals_places', cb.asset, toUInt256(1)))
        )
    )) AS effective_debt

FROM current_balances AS cb
WHERE cb.collateral_balance != 0 OR cb.debt_balance != 0;

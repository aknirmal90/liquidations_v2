-- Create view for user asset balances with interest accrual
-- This view calculates accrued balances per user and asset
-- Used as input for view_user_health_factor (146)
--
-- Stores:
-- - Raw balances (collateral_balance, debt_balance)
-- - Interest accrual factors (collateral_interest_accrual_factor, debt_interest_accrual_factor)
-- - Accrued balances as integers: int(balance * accrual_factor)
--
-- Interest Accrual Factor:
-- - Accounts for interest accrued between last index update and latest block
-- - Factor = 1 + (interest_rate / RAY / seconds_in_year * (latest_block - updated_at_block) * 12)

CREATE VIEW IF NOT EXISTS aave_ethereum.view_future_user_asset_effective_balances AS
WITH
network_info AS (
    SELECT
        dictGetOrDefault('aave_ethereum.NetworkBlockInfoDictionary', 'latest_block_number', toUInt8(1), toUInt64(0)) AS latest_block_number
),
current_balances AS (
    SELECT
        lb.user,
        lb.asset,
        -- Convert scaled balance to underlying: floor((scaled * liquidityIndex) / RAY), using Int256 to avoid overflow
        floor((toInt256(lb.collateral_scaled_balance) * toInt256(dictGetOrDefault('aave_ethereum.dict_collateral_liquidity_index', 'liquidityIndex', lb.asset, toUInt256(0)))) / toInt256('1000000000000000000000000000')) AS collateral_balance,
        floor((toInt256(lb.variable_debt_scaled_balance) * toInt256(dictGetOrDefault('aave_ethereum.dict_debt_liquidity_index', 'liquidityIndex', lb.asset, toUInt256(0)))) / toInt256('1000000000000000000000000000')) AS debt_balance,
        dictGetOrDefault('aave_ethereum.dict_collateral_liquidity_index', 'interest_rate', lb.asset, toUInt256(0)) AS collateral_interest_rate,
        dictGetOrDefault('aave_ethereum.dict_collateral_liquidity_index', 'updated_at_block', lb.asset, toUInt64(0)) AS collateral_updated_at_block,
        dictGetOrDefault('aave_ethereum.dict_debt_liquidity_index', 'interest_rate', lb.asset, toUInt256(0)) AS debt_interest_rate,
        dictGetOrDefault('aave_ethereum.dict_debt_liquidity_index', 'updated_at_block', lb.asset, toUInt64(0)) AS debt_updated_at_block
    FROM aave_ethereum.LatestBalances_v2_Memory AS lb
),
accrual_factors AS (
    SELECT
        cb.user,
        cb.asset,
        cb.collateral_balance,
        cb.debt_balance,
        ni.latest_block_number,
        cb.collateral_updated_at_block,
        cb.debt_updated_at_block,
        -- Collateral interest accrual factor
        (
            1 +
            (
                toFloat64(cb.collateral_interest_rate) / 1e27
                / 31536000
                * toFloat64(GREATEST(ni.latest_block_number - cb.collateral_updated_at_block + 1, 0))
                * 12
            )
        ) AS collateral_interest_accrual_factor,
        -- Debt interest accrual factor
        (
            1 +
            (
                toFloat64(cb.debt_interest_rate) / 1e27
                / 31536000
                * toFloat64(GREATEST(ni.latest_block_number - cb.debt_updated_at_block + 1, 0))
                * 12
            )
        ) AS debt_interest_accrual_factor
    FROM current_balances AS cb
    CROSS JOIN network_info AS ni
)
SELECT
    user,
    asset,
    toDecimal256(collateral_balance, 0) AS collateral_balance,
    toDecimal256(debt_balance, 0) AS debt_balance,
    toDecimal256(collateral_interest_accrual_factor, 18) AS collateral_interest_accrual_factor,
    toDecimal256(debt_interest_accrual_factor, 18) AS debt_interest_accrual_factor,
    -- Store accrued balances as integers: int(balance * accrual_factor)
    toDecimal256(floor(toDecimal256(collateral_balance, 0) * toDecimal256(collateral_interest_accrual_factor, 18)), 0) AS accrued_collateral_balance,
    toDecimal256(floor(toDecimal256(debt_balance, 0) * toDecimal256(debt_interest_accrual_factor, 18)), 0) AS accrued_debt_balance
FROM accrual_factors
WHERE collateral_balance != 0 OR debt_balance != 0;

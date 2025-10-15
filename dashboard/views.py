from datetime import datetime

from bokeh.embed import components
from bokeh.models import ColumnDataSource, DatetimeTickFormatter, HoverTool
from bokeh.plotting import figure
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from oracles.contracts.interface import PriceOracleInterface
from utils.clickhouse.client import clickhouse_client
from utils.constants import NETWORK_ID, NETWORK_NAME
from utils.event_parser import parse_transaction_logs
from utils.rpc import rpc_adapter
from utils.simulation import get_simulated_health_factor


def get_simple_explorer_url(address_id: str):
    """Simple explorer URL function for frontend templates"""
    if not address_id:
        return None

    if "ethereum" in NETWORK_NAME:
        return f"https://etherscan.io/address/{address_id}"
    elif "polygon" in NETWORK_NAME:
        return f"https://polygonscan.com/address/{address_id}"
    elif "avalanche" in NETWORK_NAME:
        return f"https://snowtrace.io/address/{address_id}"
    elif "tron" in NETWORK_NAME:
        return f"https://tronscan.org/#/address/{address_id}"
    elif "arbitrum" in NETWORK_NAME:
        return f"https://arbiscan.io/address/{address_id}"
    else:
        return None


def get_simple_transaction_url(tx_hash: str):
    """Simple transaction URL function for frontend templates"""
    if not tx_hash:
        return None

    if "ethereum" in NETWORK_NAME:
        return f"https://etherscan.io/tx/{tx_hash}"
    elif "polygon" in NETWORK_NAME:
        return f"https://polygonscan.com/tx/{tx_hash}"
    elif "avalanche" in NETWORK_NAME:
        return f"https://snowtrace.io/tx/{tx_hash}"
    elif "tron" in NETWORK_NAME:
        return f"https://tronscan.org/#/transaction/{tx_hash}"
    elif "arbitrum" in NETWORK_NAME:
        return f"https://arbiscan.io/tx/{tx_hash}"
    else:
        return None


@login_required
def asset_list(request):
    """View to list all assets in the database"""
    try:
        # Query all assets from the LatestAssetConfiguration view with price data and avg refresh time
        query = """
        SELECT
            ac.asset,
            ac.name,
            ac.symbol,
            ac.decimals,
            ac.collateralLTV,
            ac.collateralLiquidationThreshold,
            ac.collateralLiquidationBonus,
            lpe.historical_price_usd,
            lpe.name as price_event_name,
            -- Calculate average refresh time: (total time span) / (number of intervals)
            -- Number of intervals = number of events - 1
            CASE
                WHEN price_events.event_count >= 2 THEN
                    round(
                        dateDiff('second',
                            price_events.oldest_timestamp,
                            price_events.newest_timestamp
                        ) / (price_events.event_count - 1),
                        1
                    )
                ELSE NULL
            END as avg_refresh_time_seconds
        FROM aave_ethereum.view_LatestAssetConfiguration ac
        LEFT JOIN aave_ethereum.LatestPriceEvent lpe ON ac.asset = lpe.asset
        LEFT JOIN (
            SELECT
                asset,
                count() as event_count,
                min(blockTimestamp) as oldest_timestamp,
                max(blockTimestamp) as newest_timestamp
            FROM aave_ethereum.EventRawNumerator
            WHERE blockTimestamp > now() - INTERVAL 30 DAY
            GROUP BY asset
            HAVING event_count >= 2
        ) price_events ON ac.asset = price_events.asset
        ORDER BY ac.collateralLiquidationBonus DESC
        """

        result = clickhouse_client.execute_query(query)

        # Convert to list of dictionaries for easier template handling
        assets = []
        for row in result.result_rows:
            assets.append(
                {
                    "asset": row[0],
                    "name": row[1] or "Unknown",
                    "symbol": row[2] or "Unknown",
                    "decimals": row[3] or 18,
                    "ltv": round(row[4] / 100.0, 2) or 0,
                    "liquidation_threshold": round(row[5] / 100.0, 2) or 0,
                    "liquidation_bonus": max(
                        round((row[6] / 100.0) - 100.00, 2) or 0, 0
                    ),
                    "price_usd": row[7] if row[7] is not None else None,
                    "price_event_name": row[8] or "Unknown",
                    "avg_refresh_time": row[9] if row[9] is not None else None,
                }
            )

        return render(request, "dashboard/asset_list.html", {"assets": assets})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def asset_detail(request, asset_address):
    """View to show detailed information for a specific asset"""
    try:
        # Get asset configuration data with all token addresses
        config_query = f"""
        SELECT
            asset,
            aToken,
            variableDebtToken,
            interestRateStrategyAddress,
            name,
            symbol,
            decimals,
            decimals_places,
            collateralLTV,
            collateralLiquidationThreshold,
            collateralLiquidationBonus,
            eModeCategoryId,
            eModeLTV,
            eModeLiquidationThreshold,
            eModeLiquidationBonus
        FROM aave_ethereum.view_LatestAssetConfiguration
        WHERE asset = '{asset_address}'
        """

        config_result = clickhouse_client.execute_query(config_query)

        if not config_result.result_rows:
            return JsonResponse({"error": "Asset not found"}, status=404)

        asset_config = config_result.result_rows[0]

        # Get historical price from Event table
        event_price_query = f"""
        SELECT
            asset,
            name,
            historical_price,
            blockTimestamp,
            blockNumber,
            asset_source
        FROM aave_ethereum.LatestPriceEvent
        WHERE asset = '{asset_address}'
        """

        event_price_result = clickhouse_client.execute_query(event_price_query)
        event_price_data = None
        if event_price_result.result_rows:
            row = event_price_result.result_rows[0]
            event_price_data = {
                "name": row[1] or "Unknown",
                "price": int(row[2]) if row[2] is not None else None,
                "timestamp": row[3],
                "block_number": row[4],
                "asset_source": row[5],
                "source_type": "Event",
            }

        # Get historical price from Transaction table
        transaction_price_query = f"""
        SELECT
            asset,
            name,
            historical_price,
            blockTimestamp,
            blockNumber,
            asset_source
        FROM aave_ethereum.LatestPriceTransaction
        WHERE asset = '{asset_address}'
        """

        transaction_price_result = clickhouse_client.execute_query(
            transaction_price_query
        )
        transaction_price_data = None
        if transaction_price_result.result_rows:
            row = transaction_price_result.result_rows[0]
            transaction_price_data = {
                "price": int(row[2]) if row[2] is not None else None,
                "timestamp": row[3],
                "block_number": row[4],
                "asset_source": row[5],
                "source_type": "Transaction",
            }

        # Get predicted price
        predicted_price_query = f"""
        SELECT
            asset,
            name,
            predicted_price,
            now() AS timestamp,
            multiplier_blockNumber,
            asset_source
        FROM aave_ethereum.LatestPriceTransaction
        WHERE asset = '{asset_address}'
        """

        predicted_price_result = clickhouse_client.execute_query(predicted_price_query)
        predicted_price_data = None
        if predicted_price_result.result_rows:
            row = predicted_price_result.result_rows[0]
            predicted_price_data = {
                "price": int(row[2]) if row[2] is not None else None,
                "timestamp": row[3],
                "block_number": row[4],
                "asset_source": row[5],
                "source_type": "Predicted",
            }

        # Set current_price and asset_source for backward compatibility
        current_price = event_price_data["price"] if event_price_data else None
        asset_source = event_price_data["asset_source"] if event_price_data else None

        # Get all event data for the Events tab
        # CollateralConfigurationChanged events
        collateral_events_query = f"""
        SELECT
            asset,
            ltv,
            liquidationThreshold,
            liquidationBonus,
            blockTimestamp,
            blockNumber,
            transactionHash
        FROM aave_ethereum.CollateralConfigurationChanged
        WHERE asset = '{asset_address}'
        ORDER BY blockTimestamp DESC
        LIMIT 50
        """
        collateral_events_result = clickhouse_client.execute_query(
            collateral_events_query
        )
        collateral_events = []
        for row in collateral_events_result.result_rows:
            # Calculate formatted values
            ltv_pct = round(row[1] / 100.0, 2) if row[1] is not None else 0
            liquidation_threshold_pct = (
                round(row[2] / 100.0, 2) if row[2] is not None else 0
            )
            liquidation_bonus_calc = (
                (row[3] / 100.0) - 100.0 if row[3] is not None else 0
            )
            liquidation_bonus_pct = max(round(liquidation_bonus_calc, 2), 0)

            collateral_events.append(
                {
                    "asset": row[0],
                    "ltv": row[1],
                    "ltv_pct": ltv_pct,
                    "liquidation_threshold": row[2],
                    "liquidation_threshold_pct": liquidation_threshold_pct,
                    "liquidation_bonus": row[3],
                    "liquidation_bonus_pct": liquidation_bonus_pct,
                    "block_timestamp": row[4],
                    "block_number": row[5],
                    "transaction_hash": row[6],
                    "transaction_url": get_simple_transaction_url(row[6])
                    if row[6]
                    else None,
                }
            )

        # EModeAssetCategoryChanged events
        emode_asset_events_query = f"""
        SELECT
            asset,
            oldCategoryId,
            newCategoryId,
            blockTimestamp,
            blockNumber,
            transactionHash
        FROM aave_ethereum.EModeAssetCategoryChanged
        WHERE asset = '{asset_address}'
        ORDER BY blockTimestamp DESC
        LIMIT 50
        """
        emode_asset_events_result = clickhouse_client.execute_query(
            emode_asset_events_query
        )
        emode_asset_events = []
        for row in emode_asset_events_result.result_rows:
            emode_asset_events.append(
                {
                    "asset": row[0],
                    "old_category_id": row[1],
                    "new_category_id": row[2],
                    "block_timestamp": row[3],
                    "block_number": row[4],
                    "transaction_hash": row[5],
                    "transaction_url": get_simple_transaction_url(row[5])
                    if row[5]
                    else None,
                }
            )

        # AssetSourceUpdated events (moved from Prices tab to Events tab)
        source_events_query = f"""
        SELECT
            asset,
            source as asset_source,
            blockTimestamp,
            blockNumber,
            transactionHash
        FROM aave_ethereum.AssetSourceUpdated
        WHERE asset = '{asset_address}'
        ORDER BY blockTimestamp DESC
        LIMIT 50
        """

        source_events_result = clickhouse_client.execute_query(source_events_query)
        source_events = []
        for row in source_events_result.result_rows:
            source_events.append(
                {
                    "asset": row[0],
                    "asset_source": row[1],
                    "asset_source_url": get_simple_explorer_url(row[1])
                    if row[1]
                    else None,
                    "block_timestamp": row[2],
                    "block_number": row[3],
                    "transaction_hash": row[4],
                    "transaction_url": get_simple_transaction_url(row[4])
                    if row[4]
                    else None,
                }
            )

        # Get historical price data for timeseries
        historical_prices_query = f"""
        SELECT
            historical_price_usd,
            blockTimestamp
        FROM aave_ethereum.LatestPriceEvent
        WHERE asset = '{asset_address}'
        ORDER BY blockTimestamp DESC
        LIMIT 1000
        """

        historical_prices_result = clickhouse_client.execute_query(
            historical_prices_query
        )
        historical_prices = []
        for row in historical_prices_result.result_rows:
            try:
                price = int(row[0]) if row[0] is not None else None
                if (
                    price is not None and price > 0
                ):  # Only include valid positive prices
                    historical_prices.append({"price": price, "timestamp": row[1]})
            except (ValueError, TypeError):
                continue

        # Get historical numerator data for timeseries (Event)
        event_numerator_query = f"""
        SELECT
            blockTimestamp,
            toFloat64(numerator) as numerator_value,
            name,
            'event' as source_type
        FROM aave_ethereum.EventRawNumerator
        WHERE asset = '{asset_address}'
          AND blockTimestamp >= now() - INTERVAL 30 DAY
          AND numerator > 0
        ORDER BY blockTimestamp ASC
        LIMIT 1000
        """

        event_numerator_result = clickhouse_client.execute_query(event_numerator_query)
        event_numerator_data = []
        for row in event_numerator_result.result_rows:
            try:
                numerator_value = int(row[1]) if row[1] is not None else None
                if numerator_value is not None and numerator_value > 0:
                    event_numerator_data.append(
                        {
                            "timestamp": row[0],
                            "numerator": numerator_value,
                            "name": row[2],
                            "source_type": row[3],
                        }
                    )
            except (ValueError, TypeError):
                continue

        # Get historical numerator data for timeseries (Transaction)
        transaction_numerator_query = f"""
        SELECT
            blockTimestamp,
            toFloat64(numerator) as numerator_value,
            name,
            'transaction' as source_type
        FROM aave_ethereum.TransactionRawNumerator
        WHERE asset = '{asset_address}'
          AND blockTimestamp >= now() - INTERVAL 30 DAY
          AND numerator > 0
        ORDER BY blockTimestamp ASC
        LIMIT 1000
        """

        transaction_numerator_result = clickhouse_client.execute_query(
            transaction_numerator_query
        )
        transaction_numerator_data = []
        for row in transaction_numerator_result.result_rows:
            try:
                numerator_value = int(row[1]) if row[1] is not None else None
                if numerator_value is not None and numerator_value > 0:
                    transaction_numerator_data.append(
                        {
                            "timestamp": row[0],
                            "numerator": numerator_value,
                            "name": row[2],
                            "source_type": row[3],
                        }
                    )
            except (ValueError, TypeError):
                continue

        # Get historical multiplier data for timeseries (Event)
        event_multiplier_query = f"""
        SELECT
            blockTimestamp,
            toFloat64(multiplier) as multiplier_value,
            name,
            'event' as source_type
        FROM aave_ethereum.EventRawMultiplier
        WHERE asset = '{asset_address}'
          AND blockTimestamp >= now() - INTERVAL 30 DAY
          AND multiplier > 0
        ORDER BY blockTimestamp ASC
        LIMIT 1000
        """

        event_multiplier_result = clickhouse_client.execute_query(
            event_multiplier_query
        )
        event_multiplier_data = []
        for row in event_multiplier_result.result_rows:
            try:
                multiplier_value = int(row[1]) if row[1] is not None else None
                if multiplier_value is not None and multiplier_value > 0:
                    event_multiplier_data.append(
                        {
                            "timestamp": row[0],
                            "multiplier": multiplier_value,
                            "name": row[2],
                            "source_type": row[3],
                        }
                    )
            except (ValueError, TypeError):
                continue

        # Get historical multiplier data for timeseries (Transaction)
        transaction_multiplier_query = f"""
        SELECT
            blockTimestamp,
            toFloat64(multiplier) as multiplier_value,
            name,
            'transaction' as source_type
        FROM aave_ethereum.TransactionRawMultiplier
        WHERE asset = '{asset_address}'
          AND blockTimestamp >= now() - INTERVAL 30 DAY
          AND multiplier > 0
        ORDER BY blockTimestamp ASC
        LIMIT 1000
        """

        transaction_multiplier_result = clickhouse_client.execute_query(
            transaction_multiplier_query
        )
        transaction_multiplier_data = []
        for row in transaction_multiplier_result.result_rows:
            try:
                multiplier_value = int(row[1]) if row[1] is not None else None
                if multiplier_value is not None and multiplier_value > 0:
                    transaction_multiplier_data.append(
                        {
                            "timestamp": row[0],
                            "multiplier": multiplier_value,
                            "name": row[2],
                            "source_type": row[3],
                        }
                    )
            except (ValueError, TypeError):
                continue

        # Get historical denominator data (Event only)
        event_denominator_query = f"""
        SELECT
            blockTimestamp,
            toFloat64(denominator) as denominator_value,
            name
        FROM aave_ethereum.EventRawDenominator
        WHERE asset = '{asset_address}'
          AND blockTimestamp >= now() - INTERVAL 30 DAY
          AND denominator > 0
        ORDER BY blockTimestamp ASC
        LIMIT 1000
        """

        event_denominator_result = clickhouse_client.execute_query(
            event_denominator_query
        )
        event_denominator_data = []
        for row in event_denominator_result.result_rows:
            try:
                denominator_value = int(row[1]) if row[1] is not None else None
                if denominator_value is not None and denominator_value > 0:
                    event_denominator_data.append(
                        {
                            "timestamp": row[0],
                            "denominator": denominator_value,
                            "name": row[2],
                        }
                    )
            except (ValueError, TypeError):
                continue

        # Get historical max cap data (Event only)
        event_max_cap_query = f"""
        SELECT
            blockTimestamp,
            toFloat64(max_cap) as max_cap_value,
            name,
            max_cap_type
        FROM aave_ethereum.EventRawMaxCap
        WHERE asset = '{asset_address}'
          AND blockTimestamp >= now() - INTERVAL 30 DAY
          AND max_cap > 0
        ORDER BY blockTimestamp ASC
        LIMIT 1000
        """

        event_max_cap_result = clickhouse_client.execute_query(event_max_cap_query)
        event_max_cap_data = []
        for row in event_max_cap_result.result_rows:
            try:
                max_cap_value = int(row[1]) if row[1] is not None else None
                if max_cap_value is not None and max_cap_value > 0:
                    event_max_cap_data.append(
                        {
                            "timestamp": row[0],
                            "max_cap": max_cap_value,
                            "name": row[2],
                            "max_cap_type": int(row[3]) if row[3] is not None else 0,
                        }
                    )
            except (ValueError, TypeError):
                continue

        # Get price verification records
        price_verification_query = f"""
        SELECT
            blockTimestamp,
            pct_error,
            type,
            name
        FROM aave_ethereum.PriceVerificationRecords
        WHERE asset = '{asset_address}'
          AND blockTimestamp >= now() - INTERVAL 30 DAY
        ORDER BY blockTimestamp ASC
        LIMIT 1000
        """

        price_verification_result = clickhouse_client.execute_query(
            price_verification_query
        )
        price_verification_data = []
        for row in price_verification_result.result_rows:
            try:
                price_verification_data.append(
                    {
                        "timestamp": row[0],
                        "pct_error": float(row[1]) if row[1] is not None else 0.0,
                        "type": row[2],
                        "name": row[3],
                    }
                )
            except (ValueError, TypeError):
                continue

        # Get latest price components data (both event and transaction)
        latest_components_query = f"""
        SELECT
            e.numerator as event_numerator,
            e.denominator,
            e.max_cap,
            e.multiplier as event_multiplier,
            e.multiplier_blockNumber,
            dictGetOrDefault('aave_ethereum.MultiplierStatsDict', 'std_growth_per_sec', (e.asset, e.asset_source, e.name), CAST(0 AS Float64)) as std_growth_per_sec,
            e.name,
            e.asset_source,
            t.numerator as transaction_numerator,
            t.multiplier as transaction_multiplier,
            e.max_cap_type,
            e.max_cap_uint256,
            e.multiplier_cap
        FROM aave_ethereum.LatestPriceEvent e
        LEFT JOIN aave_ethereum.LatestPriceTransaction t ON e.asset = t.asset
        WHERE e.asset = '{asset_address}'
        LIMIT 1
        """

        latest_components_result = clickhouse_client.execute_query(
            latest_components_query
        )
        latest_components_data = None
        if latest_components_result.result_rows:
            row = latest_components_result.result_rows[0]
            try:
                # Basic component data
                components_data = {
                    "event_numerator": int(row[0]) if row[0] is not None else 0,
                    "denominator": int(row[1]) if row[1] is not None else 0,
                    "max_cap": int(row[2]) if row[2] is not None else 0,
                    "event_multiplier": int(row[3]) if row[3] is not None else 0,
                    "multiplier_blockNumber": int(row[4]) if row[4] is not None else 0,
                    "std_growth_per_sec": float(row[5]) if row[5] is not None else 0.0,
                    "name": row[6] if row[6] is not None else "Unknown",
                    "asset_source": row[7] if row[7] is not None else "Unknown",
                    "transaction_numerator": int(row[8]) if row[8] is not None else 0,
                    "transaction_multiplier": int(row[9]) if row[9] is not None else 0,
                    "max_cap_type": int(row[10]) if row[10] is not None else 0,
                    "max_cap_uint256": int(row[11]) if row[11] is not None else 0,
                    "multiplier_cap": int(row[12]) if row[12] is not None else 0,
                }

                # Add RPC price and calculated prices
                rpc_price = None
                calculated_event_price = None
                calculated_transaction_price = None
                calculated_predicted_price = None

                if components_data["asset_source"] != "Unknown":
                    try:
                        # Initialize PriceOracleInterface
                        price_oracle = PriceOracleInterface(
                            asset_address, components_data["asset_source"]
                        )

                        # Get RPC price
                        rpc_price = price_oracle.latest_price_from_rpc

                        # Get calculated prices
                        calculated_event_price = (
                            price_oracle.historical_price_from_event
                        )
                        calculated_transaction_price = (
                            price_oracle.historical_price_from_transaction
                        )
                        calculated_predicted_price = (
                            price_oracle.predicted_price_from_transaction
                        )

                    except Exception as e:
                        # Handle RPC or calculation errors gracefully
                        rpc_price = f"Error: {str(e)}"

                # Get RPC cached block height
                rpc_cached_block_height = None
                try:
                    rpc_cached_block_height = rpc_adapter.cached_block_height
                except Exception as e:
                    rpc_cached_block_height = f"Error: {str(e)}"

                components_data.update(
                    {
                        "rpc_price": rpc_price,
                        "calculated_event_price": calculated_event_price,
                        "calculated_transaction_price": calculated_transaction_price,
                        "calculated_predicted_price": calculated_predicted_price,
                        "rpc_cached_block_height": rpc_cached_block_height,
                    }
                )

                latest_components_data = components_data

            except (ValueError, TypeError):
                latest_components_data = None

        # Calculate average time between consecutive blocks
        avg_block_time = calculate_average_block_time(historical_prices)

        # Create price visualizations
        price_plots = create_price_plots(historical_prices, asset_config[6])  # symbol

        # Create price component visualizations
        price_component_plots = create_price_component_plots(
            event_numerator_data,
            transaction_numerator_data,
            event_multiplier_data,
            transaction_multiplier_data,
            event_denominator_data,
            event_max_cap_data,
            price_verification_data,
            asset_config[6],  # symbol
        )

        # Create asset plots for overview tab
        plots = create_asset_plots(asset_config, [])  # Empty historical data for now

        context = {
            "asset": {
                "address": asset_config[0],
                "aToken": asset_config[1],
                "variableDebtToken": asset_config[2],
                "interest_rate_strategy": asset_config[3],
                "name": asset_config[4] or "Unknown",
                "symbol": asset_config[5] or "Unknown",
                "price_event_name": event_price_data.get("name")
                if event_price_data
                else "Unknown",
                "decimals": asset_config[6] or 18,
                "decimals_places": asset_config[7] or 18,
                "collateral_ltv": round(asset_config[8] / 100.0, 2) or 0,
                "collateral_liquidation_threshold": round(asset_config[9] / 100.0, 2)
                or 0,
                "collateral_liquidation_bonus": max(
                    round((asset_config[10] / 100.0) - 100.00, 2) or 0, 0
                ),
                "emode_category_id": asset_config[11],
                "emode_ltv": round(asset_config[12] / 100.0, 2) or 0,
                "emode_liquidation_threshold": round(asset_config[13] / 100.0, 2) or 0,
                "emode_liquidation_bonus": max(
                    round((asset_config[14] / 100.0) - 100.00, 2) or 0, 0
                ),
                "current_price": current_price,
                "asset_source": asset_source,
                "avg_block_time": avg_block_time,
            },
            "addresses": {
                "asset": get_simple_explorer_url(asset_config[0]),
                "aToken": get_simple_explorer_url(asset_config[1]),
                "variableDebtToken": get_simple_explorer_url(asset_config[2]),
                "interest_rate_strategy": get_simple_explorer_url(asset_config[3]),
            },
            "source_events": source_events,
            "collateral_events": collateral_events,
            "emode_asset_events": emode_asset_events,
            "price_plots": price_plots,
            "price_component_plots": price_component_plots,
            "plots": plots,
            "prices": {
                "event_price": event_price_data,
                "transaction_price": transaction_price_data,
                "predicted_price": predicted_price_data,
            },
            "latest_components": latest_components_data,
        }

        return render(request, "dashboard/asset_detail.html", context)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


def calculate_average_block_time(historical_prices):
    """Calculate average time between consecutive blocks for the last 1000 records"""
    if len(historical_prices) < 2:
        return None

    # Take the last 1000 records (they're already ordered DESC)
    prices_to_analyze = historical_prices[:1000]

    # Convert timestamps to datetime objects and sort chronologically
    timestamps = []
    for price_data in prices_to_analyze:
        try:
            if isinstance(price_data["timestamp"], str):
                timestamp = datetime.fromisoformat(
                    price_data["timestamp"].replace("Z", "+00:00")
                )
            else:
                timestamp = price_data["timestamp"]
            timestamps.append(timestamp)
        except Exception as e:
            print(f"DEBUG: Error processing price data: {e}")
            continue

    if len(timestamps) < 2:
        return None

    # Sort chronologically
    timestamps.sort()

    # Calculate time differences
    time_diffs = []
    for i in range(1, len(timestamps)):
        diff = (timestamps[i] - timestamps[i - 1]).total_seconds()
        if diff > 0:  # Only include positive differences
            time_diffs.append(diff)

    if not time_diffs:
        return None

    # Calculate average
    avg_time = sum(time_diffs) / len(time_diffs)
    return round(avg_time, 2)


def create_price_plots(historical_prices, symbol):
    """Create Bokeh plots for price visualization"""
    plots = {}

    if not historical_prices:
        return plots

    # Prepare data for plotting
    prices = []
    timestamps = []

    for price_data in historical_prices:
        try:
            price = float(price_data["price"])
            if isinstance(price_data["timestamp"], str):
                timestamp = datetime.fromisoformat(
                    price_data["timestamp"].replace("Z", "+00:00")
                )
            else:
                timestamp = price_data["timestamp"]

            prices.append(price)
            timestamps.append(timestamp)
        except Exception as e:
            print(f"DEBUG: Error processing price data: {e}")
            continue

    if len(prices) < 2:
        return plots

    # Sort by timestamp
    data = list(zip(timestamps, prices))
    data.sort(key=lambda x: x[0])
    timestamps, prices = zip(*data)

    # Create price timeseries chart
    price_fig = figure(
        title=f"{symbol} Price History",
        x_axis_type="datetime",
        height=400,
        width=1000,
        tools="pan,wheel_zoom,box_zoom,reset,save",
        toolbar_location="above",
    )

    # Add hover tool
    hover = HoverTool(
        tooltips=[
            ("Date", "@x{%F %H:%M:%S}"),
            ("Price", "@y{0.000000}"),
        ],
        formatters={
            "@x": "datetime",
        },
        mode="vline",
    )
    price_fig.add_tools(hover)

    # Plot the line
    price_fig.line(
        timestamps, prices, line_width=2, color="#1f77b4", legend_label="Price"
    )
    price_fig.scatter(timestamps, prices, size=4, color="#1f77b4", alpha=0.6)

    # Format axes
    price_fig.xaxis.axis_label = "Time"
    price_fig.yaxis.axis_label = "Price (USD)"
    price_fig.xaxis.formatter = DatetimeTickFormatter(
        hours="%H:%M", days="%b %d", months="%b %Y", years="%Y"
    )
    price_fig.legend.location = "top_left"

    plots["price_chart"] = components(price_fig)

    return plots


def create_price_component_plots(
    event_numerator_data,
    transaction_numerator_data,
    event_multiplier_data,
    transaction_multiplier_data,
    event_denominator_data,
    event_max_cap_data,
    price_verification_data,
    symbol,
):
    """Create Bokeh plots for price component visualizations"""
    plots = {}

    # Create numerator chart with both event and transaction lines
    if event_numerator_data or transaction_numerator_data:
        numerator_fig = figure(
            title=f"{symbol} Numerator History (Last 30 Days)",
            x_axis_type="datetime",
            height=400,
            width=1000,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            toolbar_location="above",
        )

        # Add hover tool
        hover = HoverTool(
            tooltips=[
                ("Date", "@x{%F %H:%M:%S}"),
                ("Numerator", "@y{0,0}"),
                ("Type", "@type"),
            ],
            formatters={
                "@x": "datetime",
            },
            mode="vline",
        )
        numerator_fig.add_tools(hover)

        # Plot event numerator data
        if event_numerator_data:
            event_timestamps = []
            event_numerators = []
            for data_point in event_numerator_data:
                try:
                    numerator = int(data_point["numerator"])
                    if isinstance(data_point["timestamp"], str):
                        timestamp = datetime.fromisoformat(
                            data_point["timestamp"].replace("Z", "+00:00")
                        )
                    else:
                        timestamp = data_point["timestamp"]
                    event_timestamps.append(timestamp)
                    event_numerators.append(numerator)
                except Exception:
                    continue

            if event_timestamps:
                numerator_fig.line(
                    event_timestamps,
                    event_numerators,
                    line_width=2,
                    color="#f59e0b",
                    legend_label="Event Numerator",
                )
                numerator_fig.scatter(
                    event_timestamps,
                    event_numerators,
                    size=4,
                    color="#f59e0b",
                    alpha=0.6,
                )

        # Plot transaction numerator data
        if transaction_numerator_data:
            transaction_timestamps = []
            transaction_numerators = []
            for data_point in transaction_numerator_data:
                try:
                    numerator = int(data_point["numerator"])
                    if isinstance(data_point["timestamp"], str):
                        timestamp = datetime.fromisoformat(
                            data_point["timestamp"].replace("Z", "+00:00")
                        )
                    else:
                        timestamp = data_point["timestamp"]
                    transaction_timestamps.append(timestamp)
                    transaction_numerators.append(numerator)
                except Exception:
                    continue

            if transaction_timestamps:
                numerator_fig.line(
                    transaction_timestamps,
                    transaction_numerators,
                    line_width=2,
                    color="#d97706",
                    legend_label="Transaction Numerator",
                )
                numerator_fig.scatter(
                    transaction_timestamps,
                    transaction_numerators,
                    size=4,
                    color="#d97706",
                    alpha=0.6,
                )

        # Format axes
        numerator_fig.xaxis.axis_label = "Time"
        numerator_fig.yaxis.axis_label = "Numerator Value"
        numerator_fig.xaxis.formatter = DatetimeTickFormatter(
            hours="%H:%M", days="%b %d", months="%b %Y", years="%Y"
        )
        numerator_fig.legend.location = "top_left"

        plots["numerator_chart"] = components(numerator_fig)

    # Create multiplier chart with both event and transaction lines
    if event_multiplier_data or transaction_multiplier_data:
        multiplier_fig = figure(
            title=f"{symbol} Multiplier History (Last 30 Days)",
            x_axis_type="datetime",
            height=400,
            width=1000,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            toolbar_location="above",
        )

        # Add hover tool
        hover = HoverTool(
            tooltips=[
                ("Date", "@x{%F %H:%M:%S}"),
                ("Multiplier", "@y{0,0}"),
                ("Type", "@type"),
            ],
            formatters={
                "@x": "datetime",
            },
            mode="vline",
        )
        multiplier_fig.add_tools(hover)

        # Plot event multiplier data
        if event_multiplier_data:
            event_timestamps = []
            event_multipliers = []
            for data_point in event_multiplier_data:
                try:
                    multiplier = int(data_point["multiplier"])
                    if isinstance(data_point["timestamp"], str):
                        timestamp = datetime.fromisoformat(
                            data_point["timestamp"].replace("Z", "+00:00")
                        )
                    else:
                        timestamp = data_point["timestamp"]
                    event_timestamps.append(timestamp)
                    event_multipliers.append(multiplier)
                except Exception:
                    continue

            if event_timestamps:
                multiplier_fig.line(
                    event_timestamps,
                    event_multipliers,
                    line_width=2,
                    color="#8b5cf6",
                    legend_label="Event Multiplier",
                )
                multiplier_fig.scatter(
                    event_timestamps,
                    event_multipliers,
                    size=4,
                    color="#8b5cf6",
                    alpha=0.6,
                )

        # Plot transaction multiplier data
        if transaction_multiplier_data:
            transaction_timestamps = []
            transaction_multipliers = []
            for data_point in transaction_multiplier_data:
                try:
                    multiplier = int(data_point["multiplier"])
                    if isinstance(data_point["timestamp"], str):
                        timestamp = datetime.fromisoformat(
                            data_point["timestamp"].replace("Z", "+00:00")
                        )
                    else:
                        timestamp = data_point["timestamp"]
                    transaction_timestamps.append(timestamp)
                    transaction_multipliers.append(multiplier)
                except Exception:
                    continue

            if transaction_timestamps:
                multiplier_fig.line(
                    transaction_timestamps,
                    transaction_multipliers,
                    line_width=2,
                    color="#3b82f6",
                    legend_label="Transaction Multiplier",
                )
                multiplier_fig.scatter(
                    transaction_timestamps,
                    transaction_multipliers,
                    size=4,
                    color="#3b82f6",
                    alpha=0.6,
                )

        # Format axes
        multiplier_fig.xaxis.axis_label = "Time"
        multiplier_fig.yaxis.axis_label = "Multiplier Value"
        multiplier_fig.xaxis.formatter = DatetimeTickFormatter(
            hours="%H:%M", days="%b %d", months="%b %Y", years="%Y"
        )
        multiplier_fig.legend.location = "top_left"

        plots["multiplier_chart"] = components(multiplier_fig)

    # Create denominator chart
    if event_denominator_data:
        denominator_fig = figure(
            title=f"{symbol} Denominator History (Last 30 Days)",
            x_axis_type="datetime",
            height=400,
            width=1000,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            toolbar_location="above",
        )

        # Add hover tool
        hover = HoverTool(
            tooltips=[
                ("Date", "@x{%F %H:%M:%S}"),
                ("Denominator", "@y{0,0}"),
            ],
            formatters={
                "@x": "datetime",
            },
            mode="vline",
        )
        denominator_fig.add_tools(hover)

        # Prepare denominator data
        denominator_timestamps = []
        denominators = []
        for data_point in event_denominator_data:
            try:
                denominator = int(data_point["denominator"])
                if isinstance(data_point["timestamp"], str):
                    timestamp = datetime.fromisoformat(
                        data_point["timestamp"].replace("Z", "+00:00")
                    )
                else:
                    timestamp = data_point["timestamp"]
                denominator_timestamps.append(timestamp)
                denominators.append(denominator)
            except Exception:
                continue

        if denominator_timestamps:
            denominator_fig.line(
                denominator_timestamps,
                denominators,
                line_width=2,
                color="#10b981",
                legend_label="Event Denominator",
            )
            denominator_fig.scatter(
                denominator_timestamps, denominators, size=4, color="#10b981", alpha=0.6
            )

        # Format axes
        denominator_fig.xaxis.axis_label = "Time"
        denominator_fig.yaxis.axis_label = "Denominator Value"
        denominator_fig.xaxis.formatter = DatetimeTickFormatter(
            hours="%H:%M", days="%b %d", months="%b %Y", years="%Y"
        )
        denominator_fig.legend.location = "top_left"

        plots["denominator_chart"] = components(denominator_fig)

    # Create max cap chart
    if event_max_cap_data:
        max_cap_fig = figure(
            title=f"{symbol} Max Cap History (Last 30 Days)",
            x_axis_type="datetime",
            height=400,
            width=1000,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            toolbar_location="above",
        )

        # Add hover tool
        hover = HoverTool(
            tooltips=[
                ("Date", "@x{%F %H:%M:%S}"),
                ("Max Cap", "@y{0,0}"),
            ],
            formatters={
                "@x": "datetime",
            },
            mode="vline",
        )
        max_cap_fig.add_tools(hover)

        # Prepare max cap data
        max_cap_timestamps = []
        max_caps = []
        for data_point in event_max_cap_data:
            try:
                max_cap = int(data_point["max_cap"])
                if isinstance(data_point["timestamp"], str):
                    timestamp = datetime.fromisoformat(
                        data_point["timestamp"].replace("Z", "+00:00")
                    )
                else:
                    timestamp = data_point["timestamp"]
                max_cap_timestamps.append(timestamp)
                max_caps.append(max_cap)
            except Exception:
                continue

        if max_cap_timestamps:
            max_cap_fig.line(
                max_cap_timestamps,
                max_caps,
                line_width=2,
                color="#ef4444",
                legend_label="Event Max Cap",
            )
            max_cap_fig.scatter(
                max_cap_timestamps, max_caps, size=4, color="#ef4444", alpha=0.6
            )

        # Format axes
        max_cap_fig.xaxis.axis_label = "Time"
        max_cap_fig.yaxis.axis_label = "Max Cap Value"
        max_cap_fig.xaxis.formatter = DatetimeTickFormatter(
            hours="%H:%M", days="%b %d", months="%b %Y", years="%Y"
        )
        max_cap_fig.legend.location = "top_left"

        plots["max_cap_chart"] = components(max_cap_fig)

    # Create price verification chart
    if price_verification_data:
        verification_fig = figure(
            title=f"{symbol} Price Verification Error % (Last 30 Days)",
            x_axis_type="datetime",
            height=400,
            width=1000,
            tools="pan,wheel_zoom,box_zoom,reset,save",
            toolbar_location="above",
        )

        # Add hover tool
        hover = HoverTool(
            tooltips=[
                ("Date", "@x{%F %H:%M:%S}"),
                ("Error %", "@y{0.00}%"),
                ("Type", "@type"),
            ],
            formatters={
                "@x": "datetime",
            },
            mode="vline",
        )
        verification_fig.add_tools(hover)

        # Group data by type
        historical_event_data = []
        historical_transaction_data = []
        predicted_transaction_data = []

        for data_point in price_verification_data:
            try:
                pct_error = float(data_point["pct_error"])
                if isinstance(data_point["timestamp"], str):
                    timestamp = datetime.fromisoformat(
                        data_point["timestamp"].replace("Z", "+00:00")
                    )
                else:
                    timestamp = data_point["timestamp"]

                if data_point["type"] == "historical_event":
                    historical_event_data.append((timestamp, pct_error))
                elif data_point["type"] == "historical_transaction":
                    historical_transaction_data.append((timestamp, pct_error))
                elif data_point["type"] == "predicted_transaction":
                    predicted_transaction_data.append((timestamp, pct_error))
            except Exception:
                continue

        # Plot historical_event data
        if historical_event_data:
            historical_event_data.sort(key=lambda x: x[0])
            event_timestamps, event_errors = zip(*historical_event_data)
            verification_fig.line(
                event_timestamps,
                event_errors,
                line_width=2,
                color="#8b5cf6",
                legend_label="Historical Event",
            )
            verification_fig.scatter(
                event_timestamps, event_errors, size=4, color="#8b5cf6", alpha=0.6
            )

        # Plot historical_transaction data
        if historical_transaction_data:
            historical_transaction_data.sort(key=lambda x: x[0])
            transaction_timestamps, transaction_errors = zip(
                *historical_transaction_data
            )
            verification_fig.line(
                transaction_timestamps,
                transaction_errors,
                line_width=2,
                color="#3b82f6",
                legend_label="Historical Transaction",
            )
            verification_fig.scatter(
                transaction_timestamps,
                transaction_errors,
                size=4,
                color="#3b82f6",
                alpha=0.6,
            )

        # Plot predicted_transaction data
        if predicted_transaction_data:
            predicted_transaction_data.sort(key=lambda x: x[0])
            predicted_timestamps, predicted_errors = zip(*predicted_transaction_data)
            verification_fig.line(
                predicted_timestamps,
                predicted_errors,
                line_width=2,
                color="#10b981",
                legend_label="Predicted Transaction",
            )
            verification_fig.scatter(
                predicted_timestamps,
                predicted_errors,
                size=4,
                color="#10b981",
                alpha=0.6,
            )

        # Format axes
        verification_fig.xaxis.axis_label = "Time"
        verification_fig.yaxis.axis_label = "Error Percentage (%)"
        verification_fig.xaxis.formatter = DatetimeTickFormatter(
            hours="%H:%M", days="%b %d", months="%b %Y", years="%Y"
        )
        verification_fig.legend.location = "top_left"

        plots["verification_chart"] = components(verification_fig)

    return plots


def create_asset_plots(asset_config, historical_data):
    """Create Bokeh plots for asset visualization"""
    plots = {}

    # Create historical events chart if data exists
    if historical_data:
        dates = [str(row[0]) for row in historical_data]
        counts = [row[1] for row in historical_data]

        # Reverse to show chronological order
        dates.reverse()
        counts.reverse()

        history_fig = figure(
            title=f"{asset_config[6]} Historical Events",  # symbol
            x_axis_type="datetime",
            height=400,
            width=600,
            tools="pan,wheel_zoom,box_zoom,reset,save",
        )

        history_fig.line(dates, counts, line_width=2, color="#2ca02c")
        history_fig.scatter(dates, counts, size=8, color="#2ca02c", alpha=0.7)

        history_fig.xaxis.axis_label = "Date"
        history_fig.yaxis.axis_label = "Event Count"

        plots["history_chart"] = components(history_fig)

    return plots


@login_required
@csrf_exempt
@require_http_methods(["GET"])
def asset_data_api(request, asset_address):
    """API endpoint to get asset data for AJAX requests"""
    try:
        query = f"""
        SELECT
            asset,
            name,
            symbol,
            decimals,
            collateralLTV,
            collateralLiquidationThreshold,
            collateralLiquidationBonus,
            eModeCategoryId,
            eModeLTV,
            eModeLiquidationThreshold,
            eModeLiquidationBonus
        FROM aave_ethereum.view_LatestAssetConfiguration
        WHERE asset = '{asset_address}'
        """

        result = clickhouse_client.execute_query(query)

        if not result.result_rows:
            return JsonResponse({"error": "Asset not found"}, status=404)

        row = result.result_rows[0]
        data = {
            "asset": row[0],
            "name": row[1] or "Unknown",
            "symbol": row[2] or "Unknown",
            "decimals": row[3] or 18,
            "collateral_ltv": row[4] or 0,
            "collateral_liquidation_threshold": row[5] or 0,
            "collateral_liquidation_bonus": row[6] or 0,
            "emode_category_id": row[7],
            "emode_ltv": row[8] or 0,
            "emode_liquidation_threshold": row[9] or 0,
            "emode_liquidation_bonus": row[10] or 0,
        }

        return JsonResponse(data)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def prices_summary(request):
    """View to show price verification summary across all assets"""
    try:
        # Get initial box plot data for default time window (1 hour)
        time_window = request.GET.get("time_window", "1_hour")

        # Get box plot data directly without going through the API
        box_plot_json = get_box_plot_data(time_window)

        # Create box plots
        box_plots = create_box_plots(box_plot_json["data"])

        # Generate Bokeh components for box plots
        box_plot_scripts = {}
        box_plot_divs = {}

        for price_type, plot in box_plots.items():
            script, div = components(plot)
            box_plot_scripts[price_type] = script
            box_plot_divs[price_type] = div
        # Query to get latest price verification records by asset, source, and name
        # grouped by type to show error percentages for each price type
        query = """
        WITH LatestRecords AS (
            SELECT
                asset,
                asset_source,
                name,
                type,
                pct_error,
                blockTimestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY asset, type
                    ORDER BY blockTimestamp DESC
                ) as rn
            FROM aave_ethereum.PriceVerificationRecords
            WHERE blockTimestamp >= now() - INTERVAL 30 DAY
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY name, asset, asset_source) as row_number,
            asset,
            asset_source,
            name,
            SUM(CASE WHEN type = 'historical_event' THEN pct_error ELSE NULL END) as historical_event_error,
            SUM(CASE WHEN type = 'historical_transaction' THEN pct_error ELSE NULL END) as historical_transaction_error,
            SUM(CASE WHEN type = 'predicted_transaction' THEN pct_error ELSE NULL END) as predicted_transaction_error
        FROM LatestRecords
        WHERE rn = 1
        GROUP BY asset, asset_source, name
        ORDER BY name, asset, asset_source
        """

        result = clickhouse_client.execute_query(query)

        # Convert to list of dictionaries for easier template handling
        price_records = []
        unique_assets = set()
        unique_sources = set()

        for row in result.result_rows:
            unique_assets.add(row[1])
            unique_sources.add(row[2])

            price_records.append(
                {
                    "row_number": row[0],
                    "asset": row[1],
                    "asset_source": row[2],
                    "name": row[3],
                    "historical_event_error": round(row[4] * 10000, 2)
                    if row[4] is not None
                    else None,
                    "historical_transaction_error": round(row[5] * 10000, 2)
                    if row[5] is not None
                    else None,
                    "predicted_transaction_error": round(row[6] * 10000, 2)
                    if row[6] is not None
                    else None,
                    "asset_url": get_simple_explorer_url(row[1]) if row[1] else None,
                    "asset_source_url": get_simple_explorer_url(row[2])
                    if row[2]
                    else None,
                }
            )

        context = {
            "price_records": price_records,
            "unique_assets_count": len(unique_assets),
            "unique_sources_count": len(unique_sources),
            "box_plot_scripts": box_plot_scripts,
            "box_plot_divs": box_plot_divs,
            "current_time_window": time_window,
        }

        return render(request, "dashboard/prices_summary.html", context)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


def get_box_plot_data(time_window="1_hour"):
    """Helper function to get box plot data for price verification errors"""
    # Map time window to SQL interval
    interval_map = {
        "1_hour": "1 HOUR",
        "1_day": "1 DAY",
        "1_week": "7 DAY",
        "1_month": "30 DAY",
    }

    interval = interval_map.get(time_window, "30 DAY")

    # Query to get all price verification records for box plots
    query = f"""
    SELECT
        name,
        type,
        pct_error,
        blockTimestamp
    FROM aave_ethereum.PriceVerificationRecords
    WHERE blockTimestamp >= now() - INTERVAL {interval}
      AND pct_error IS NOT NULL
    ORDER BY name, type, blockTimestamp
    """

    result = clickhouse_client.execute_query(query)

    # Group data by name and type for box plots
    box_plot_data = {}
    for row in result.result_rows:
        name = row[0]
        price_type = row[1]
        pct_error = float(row[2])

        if name not in box_plot_data:
            box_plot_data[name] = {}

        if price_type not in box_plot_data[name]:
            box_plot_data[name][price_type] = []

        box_plot_data[name][price_type].append(pct_error)

    return {"data": box_plot_data, "time_window": time_window}


@login_required
def price_box_plot_data(request):
    """API endpoint to get box plot data for price verification errors"""
    try:
        time_window = request.GET.get("time_window", "1_month")
        return JsonResponse(get_box_plot_data(time_window))

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def price_mismatch_counts_data(request):
    """API endpoint to get mismatch counts timeseries data"""
    try:
        time_window = request.GET.get("time_window", "1_day")

        # Convert time window to ClickHouse interval
        interval_map = {
            "1_hour": "1 HOUR",
            "1_day": "1 DAY",
            "1_week": "7 DAY",
            "1_month": "30 DAY",
        }

        interval = interval_map.get(time_window, "1 DAY")

        query = f"""
        SELECT
            toDateTime(insert_timestamp) as timestamp,
            historical_event_vs_rpc,
            historical_transaction_vs_rpc,
            predicted_transaction_vs_rpc,
            total_assets_verified,
            total_assets_different
        FROM aave_ethereum.PriceMismatchCounts
        WHERE insert_timestamp >= now() - INTERVAL {interval}
        ORDER BY insert_timestamp DESC
        """

        result = clickhouse_client.execute_query(query)

        # Convert to list of dictionaries for JSON response
        data = []
        for row in result.result_rows:
            data.append(
                {
                    "timestamp": row[0].isoformat() if row[0] else None,
                    "historical_event_vs_rpc": row[1],
                    "historical_transaction_vs_rpc": row[2],
                    "predicted_transaction_vs_rpc": row[3],
                    "total_assets_verified": row[4],
                    "total_assets_different": row[5],
                }
            )

        return JsonResponse({"data": data})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def price_zero_error_stats_data(request):
    """API endpoint to get zero error statistics by type"""
    try:
        time_window = request.GET.get("time_window", "1_hour")

        # Convert time window to ClickHouse interval
        interval_map = {
            "1_hour": "1 HOUR",
            "1_day": "1 DAY",
            "1_week": "7 DAY",
            "1_month": "30 DAY",
        }

        interval = interval_map.get(time_window, "1 HOUR")

        query = f"""
        SELECT
            name as error_type,
            COUNT(*) as total_records,
            SUM(CASE WHEN type = 'historical_event' AND ABS(pct_error) < 0.000001 THEN 1 ELSE 0 END) as historical_event_valid_count,
            SUM(CASE WHEN type = 'historical_transaction' AND ABS(pct_error) < 0.000001 THEN 1 ELSE 0 END) as historical_transaction_valid_count,
            SUM(CASE WHEN type = 'predicted_transaction' AND ABS(pct_error) < 0.000001 THEN 1 ELSE 0 END) as predicted_transaction_valid_count,
            SUM(CASE WHEN type = 'historical_event' THEN 1 ELSE 0 END) as historical_event_total_count,
            SUM(CASE WHEN type = 'historical_transaction' THEN 1 ELSE 0 END) as historical_transaction_total_count,
            SUM(CASE WHEN type = 'predicted_transaction' THEN 1 ELSE 0 END) as predicted_transaction_total_count,
            CASE
                WHEN SUM(CASE WHEN type = 'historical_event' THEN 1 ELSE 0 END) > 0
                THEN ROUND((SUM(CASE WHEN type = 'historical_event' AND ABS(pct_error) < 0.000001 THEN 1 ELSE 0 END) * 100.0 / SUM(CASE WHEN type = 'historical_event' THEN 1 ELSE 0 END)), 2)
                ELSE 0.0
            END as historical_event_valid_percentage,
            CASE
                WHEN SUM(CASE WHEN type = 'historical_transaction' THEN 1 ELSE 0 END) > 0
                THEN ROUND((SUM(CASE WHEN type = 'historical_transaction' AND ABS(pct_error) < 0.000001 THEN 1 ELSE 0 END) * 100.0 / SUM(CASE WHEN type = 'historical_transaction' THEN 1 ELSE 0 END)), 2)
                ELSE 0.0
            END as historical_transaction_valid_percentage,
            CASE
                WHEN SUM(CASE WHEN type = 'predicted_transaction' THEN 1 ELSE 0 END) > 0
                THEN ROUND((SUM(CASE WHEN type = 'predicted_transaction' AND ABS(pct_error) < 0.000001 THEN 1 ELSE 0 END) * 100.0 / SUM(CASE WHEN type = 'predicted_transaction' THEN 1 ELSE 0 END)), 2)
                ELSE 0.0
            END as predicted_transaction_valid_percentage
        FROM aave_ethereum.PriceVerificationRecords
        WHERE blockTimestamp >= now() - INTERVAL {interval}
        AND pct_error IS NOT NULL
        GROUP BY name
        ORDER BY name
        """

        result = clickhouse_client.execute_query(query)

        # Convert to list of dictionaries for JSON response
        data = []
        for row in result.result_rows:
            data.append(
                {
                    "error_type": row[0],
                    "total_records": row[1],
                    "historical_event_zero_count": row[2],
                    "historical_transaction_zero_count": row[3],
                    "predicted_transaction_zero_count": row[4],
                    "historical_event_total_count": row[5],
                    "historical_transaction_total_count": row[6],
                    "predicted_transaction_total_count": row[7],
                    "historical_event_zero_percentage": row[8],
                    "historical_transaction_zero_percentage": row[9],
                    "predicted_transaction_zero_percentage": row[10],
                }
            )

        return JsonResponse({"data": data})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def transaction_coverage_metrics(request):
    """API endpoint to get transaction coverage metrics"""
    try:
        time_window = request.GET.get("time_window", "1_hour")

        # Convert time window to ClickHouse interval
        interval_map = {
            "1_hour": "1 HOUR",
            "1_day": "1 DAY",
            "1_week": "7 DAY",
            "1_month": "30 DAY",
        }

        interval = interval_map.get(time_window, "1 HOUR")

        # Query to analyze transaction coverage
        # Only include latest asset/asset_source combinations
        coverage_query = f"""
        WITH latest_asset_sources AS (
            SELECT asset, source as asset_source
            FROM aave_ethereum.LatestAssetSourceUpdated FINAL
            GROUP BY asset, source
        ),
        transaction_analysis AS (
            SELECT
                t.transactionHash,
                t.asset,
                t.asset_source,
                SUM(CASE WHEN t.type = 'event' THEN 1 ELSE 0 END) as has_event,
                SUM(CASE WHEN t.type = 'transaction' THEN 1 ELSE 0 END) as has_transaction
            FROM aave_ethereum.TransactionRawNumerator t
            INNER JOIN latest_asset_sources las
                ON t.asset = las.asset AND t.asset_source = las.asset_source
            WHERE t.blockTimestamp >= now() - INTERVAL {interval}
            GROUP BY t.transactionHash, t.asset, t.asset_source
        )
        SELECT
            SUM(CASE WHEN has_event > 0 AND has_transaction = 0 THEN 1 ELSE 0 END) as event_only,
            SUM(CASE WHEN has_event = 0 AND has_transaction > 0 THEN 1 ELSE 0 END) as transaction_only,
            SUM(CASE WHEN has_event > 0 AND has_transaction > 0 THEN 1 ELSE 0 END) as both_types
        FROM transaction_analysis
        """

        result = clickhouse_client.execute_query(coverage_query)

        if result.result_rows:
            row = result.result_rows[0]
            data = {
                "event_only": row[0],
                "transaction_only": row[1],
                "both_types": row[2],
            }
        else:
            data = {"event_only": 0, "transaction_only": 0, "both_types": 0}

        return JsonResponse(data)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def transaction_timestamp_differences(request):
    """API endpoint to get timestamp differences for transactions with both types"""
    try:
        time_window = request.GET.get("time_window", "1_hour")

        # Convert time window to ClickHouse interval
        interval_map = {
            "1_hour": "1 HOUR",
            "1_day": "1 DAY",
            "1_week": "7 DAY",
            "1_month": "30 DAY",
        }

        interval = interval_map.get(time_window, "1 HOUR")

        # Query to get aggregated timestamp differences for transactions with both types
        # Only include latest asset/asset_source combinations
        diff_query = f"""
        WITH latest_asset_sources AS (
            SELECT asset, source as asset_source
            FROM aave_ethereum.LatestAssetSourceUpdated FINAL
            GROUP BY asset, source
        ),
        transaction_pairs AS (
            SELECT
                t.asset,
                t.asset_source,
                t.name,
                t.transactionHash,
                MAX(CASE WHEN t.type = 'event' THEN t.blockTimestamp ELSE NULL END) as event_timestamp,
                MAX(CASE WHEN t.type = 'transaction' THEN t.blockTimestamp ELSE NULL END) as transaction_timestamp
            FROM aave_ethereum.TransactionRawNumerator t
            INNER JOIN latest_asset_sources las
                ON t.asset = las.asset AND t.asset_source = las.asset_source
            WHERE t.blockTimestamp >= now() - INTERVAL {interval}
            GROUP BY t.asset, t.asset_source, t.name, t.transactionHash
            HAVING event_timestamp IS NOT NULL AND transaction_timestamp IS NOT NULL
        ),
        timestamp_diffs AS (
            SELECT
                asset,
                asset_source,
                name,
                (toInt64(transaction_timestamp) - toInt64(event_timestamp)) / 1000000.0 as timestamp_diff_seconds
            FROM transaction_pairs
        )
        SELECT
            asset,
            asset_source,
            name,
            MIN(timestamp_diff_seconds) as min_diff,
            MAX(timestamp_diff_seconds) as max_diff,
            AVG(timestamp_diff_seconds) as avg_diff,
            COUNT(*) as count_transactions
        FROM timestamp_diffs
        GROUP BY asset, asset_source, name
        ORDER BY asset, asset_source, name
        """

        result = clickhouse_client.execute_query(diff_query)

        # Convert to list of dictionaries for JSON response
        data = []
        for row in result.result_rows:
            data.append(
                {
                    "asset": row[0],
                    "asset_source": row[1],
                    "name": row[2],
                    "min_diff": float(row[3]) if row[3] is not None else 0.0,
                    "max_diff": float(row[4]) if row[4] is not None else 0.0,
                    "avg_diff": float(row[5]) if row[5] is not None else 0.0,
                    "count_transactions": int(row[6]) if row[6] is not None else 0,
                }
            )

        return JsonResponse({"data": data})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


def create_box_plots(box_plot_data):
    """Create 3 interactive box plots for price verification errors by type"""
    plots = {}

    # Reorganize data by price type
    type_data = {
        "historical_event": {},
        "historical_transaction": {},
        "predicted_transaction": {},
    }

    # Group by price type first, then by name
    for name, types_data in box_plot_data.items():
        for price_type, errors in types_data.items():
            if price_type in type_data:
                type_data[price_type][name] = errors

    # Create one plot for each price type
    for price_type, names_data in type_data.items():
        if not names_data:
            # Create empty plot with message
            p = figure(
                title=f"{price_type.replace('_', ' ').title()} Price Errors",
                width=1200,
                height=800,
                toolbar_location="above",
            )
            p.text(
                x=[0.5],
                y=[0.5],
                text=["No data available"],
                text_font_size="14pt",
                text_align="center",
                text_baseline="middle",
            )
            plots[price_type] = p
            continue

        # Get all unique names for x-axis
        names = list(names_data.keys())
        if not names:
            continue

        # Create hover tool for tooltips
        hover = HoverTool(
            tooltips=[
                ("Oracle Name", "@x"),
                ("Q1 (25%)", "@q1{0.000000}"),
                ("Median (50%)", "@median{0.000000}"),
                ("Q3 (75%)", "@q3{0.000000}"),
                ("Min", "@min{0.000000}"),
                ("Max", "@max{0.000000}"),
                ("Data Points", "@count"),
            ]
        )

        # Create figure for this price type
        p = figure(
            title=f"{price_type.replace('_', ' ').title()} Price Errors",
            x_range=names,
            width=1200,
            height=800,
            toolbar_location="above",
            tools=[hover, "pan", "wheel_zoom", "box_zoom", "reset", "save"],
        )

        # Prepare data for ColumnDataSource
        box_data = {
            "x": [],
            "q1": [],
            "median": [],
            "q3": [],
            "min": [],
            "max": [],
            "count": [],
            "lower_whisker": [],
            "upper_whisker": [],
        }

        # Calculate box plot statistics for each name
        for i, name in enumerate(names):
            errors = names_data[name]
            if not errors:
                continue

            errors_sorted = sorted(errors)
            n = len(errors_sorted)

            if n == 0:
                continue

            # Calculate percentiles
            q1 = errors_sorted[int(n * 0.25)]
            q2 = errors_sorted[int(n * 0.5)]  # median
            q3 = errors_sorted[int(n * 0.75)]
            iqr = q3 - q1

            # Calculate whiskers
            lower_whisker = max(min(errors_sorted), q1 - 1.5 * iqr)
            upper_whisker = min(max(errors_sorted), q3 + 1.5 * iqr)

            # Store data for tooltips
            box_data["x"].append(name)
            box_data["q1"].append(q1)
            box_data["median"].append(q2)
            box_data["q3"].append(q3)
            box_data["min"].append(min(errors_sorted))
            box_data["max"].append(max(errors_sorted))
            box_data["count"].append(n)
            box_data["lower_whisker"].append(lower_whisker)
            box_data["upper_whisker"].append(upper_whisker)

        # Create ColumnDataSource
        if box_data["x"]:
            source = ColumnDataSource(data=box_data)

            # Box plots using the data source
            p.vbar(
                x="x",
                top="q3",
                bottom="q1",
                width=0.6,
                color="lightblue",
                alpha=0.7,
                line_color="black",
                source=source,
            )

            # Add median lines and whiskers for each box
            for i, name in enumerate(box_data["x"]):
                q1 = box_data["q1"][i]
                q2 = box_data["median"][i]
                q3 = box_data["q3"][i]
                lower_whisker = box_data["lower_whisker"][i]
                upper_whisker = box_data["upper_whisker"][i]

                # Median line
                p.line(x=[i - 0.3, i + 0.3], y=[q2, q2], line_width=2, color="red")

                # Whiskers
                p.line(x=[name, name], y=[q1, lower_whisker], line_color="black")
                p.line(x=[name, name], y=[q3, upper_whisker], line_color="black")

                # Whisker caps
                p.line(
                    x=[i - 0.1, i + 0.1],
                    y=[lower_whisker, lower_whisker],
                    line_color="black",
                )
                p.line(
                    x=[i - 0.1, i + 0.1],
                    y=[upper_whisker, upper_whisker],
                    line_color="black",
                )

        p.xaxis.axis_label = "Oracle Name"
        p.yaxis.axis_label = "Error Percentage"
        p.title.text_font_size = "14pt"
        p.xaxis.major_label_orientation = 45  # Rotate labels for readability

        plots[price_type] = p

    # Ensure we always have all 3 plot types, even if empty
    expected_types = [
        "historical_event",
        "historical_transaction",
        "predicted_transaction",
    ]
    for expected_type in expected_types:
        if expected_type not in plots:
            p = figure(
                title=f"{expected_type.replace('_', ' ').title()} Price Errors",
                width=1200,
                height=800,
                toolbar_location="above",
            )
            p.text(
                x=[0.5],
                y=[0.5],
                text=["No data available"],
                text_font_size="14pt",
                text_align="center",
                text_baseline="middle",
            )
            plots[expected_type] = p

    return plots


@login_required
def liquidations(request):
    """View to show liquidations dashboard"""
    try:
        # Get time window from request, default to 1 day
        time_window = request.GET.get("time_window", "1_day")

        context = {
            "current_time_window": time_window,
        }

        return render(request, "dashboard/liquidations.html", context)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def liquidations_metrics(request):
    """API endpoint to get liquidations metrics"""
    try:
        time_window = request.GET.get("time_window", "1_day")
        min_value = request.GET.get("min_value")

        # Convert time window to ClickHouse interval
        interval_map = {
            "1_hour": "1 HOUR",
            "1_day": "1 DAY",
            "1_week": "7 DAY",
            "1_month": "30 DAY",
            "1_year": "365 DAY",
        }

        interval = interval_map.get(time_window, "1 DAY")

        # Add min_value filter condition if specified
        min_value_condition = ""
        if min_value:
            try:
                min_val = float(min_value)
                min_value_condition = f"""
                AND (
                    CASE
                        WHEN lpe.historical_price_usd > 0 AND COALESCE(tm.decimals, 18) > 0
                        THEN (toFloat64(l.liquidatedCollateralAmount) * lpe.historical_price_usd) / POW(10, COALESCE(tm.decimals, 18))
                        ELSE 0
                    END
                ) >= {min_val}
                """
            except ValueError:
                pass

        # Query for liquidations metrics
        metrics_query = f"""
        SELECT
            COUNT(*) as total_liquidations,
            COUNT(DISTINCT l.liquidator) as unique_liquidators,
            SUM(
                CASE
                    WHEN lpe.historical_price_usd > 0 AND COALESCE(tm.decimals, 18) > 0
                    THEN (toFloat64(l.liquidatedCollateralAmount) * lpe.historical_price_usd) / POW(10, COALESCE(tm.decimals, 18))
                    ELSE 0
                END
            ) as total_usd_volume
        FROM aave_ethereum.LiquidationCall l
        LEFT JOIN aave_ethereum.view_LatestAssetConfiguration tm
            ON l.collateralAsset = tm.asset
        LEFT JOIN aave_ethereum.LatestPriceEvent lpe
            ON l.collateralAsset = lpe.asset
        WHERE l.blockTimestamp >= now() - INTERVAL {interval}
        {min_value_condition}
        """

        result = clickhouse_client.execute_query(metrics_query)

        if result.result_rows:
            row = result.result_rows[0]
            data = {
                "total_liquidations": int(row[0]) if row[0] else 0,
                "unique_liquidators": int(row[1]) if row[1] else 0,
                "total_usd_volume": float(row[2]) if row[2] else 0.0,
            }
        else:
            data = {
                "total_liquidations": 0,
                "unique_liquidators": 0,
                "total_usd_volume": 0.0,
            }

        return JsonResponse(data)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def liquidations_top_liquidators(request):
    """API endpoint to get top liquidators data"""
    try:
        time_window = request.GET.get("time_window", "1_day")
        min_value = request.GET.get("min_value")

        # Convert time window to ClickHouse interval
        interval_map = {
            "1_hour": "1 HOUR",
            "1_day": "1 DAY",
            "1_week": "7 DAY",
            "1_month": "30 DAY",
            "1_year": "365 DAY",
        }

        interval = interval_map.get(time_window, "1 DAY")

        # Add min_value filter condition if specified
        min_value_condition = ""
        if min_value:
            try:
                min_val = float(min_value)
                min_value_condition = f"""
                AND (
                    CASE
                        WHEN lpe.historical_price_usd > 0 AND COALESCE(tm.decimals, 18) > 0
                        THEN (toFloat64(l.liquidatedCollateralAmount) * lpe.historical_price_usd) / POW(10, COALESCE(tm.decimals, 18))
                        ELSE 0
                    END
                ) >= {min_val}
                """
            except ValueError:
                pass

        # Query for top liquidators
        top_liquidators_query = f"""
        SELECT
            l.liquidator,
            COUNT(DISTINCT l.transactionHash) as txn_count,
            SUM(
                CASE
                    WHEN lpe.historical_price_usd > 0 AND COALESCE(tm.decimals, 18) > 0
                    THEN (toFloat64(l.liquidatedCollateralAmount) * lpe.historical_price_usd) / POW(10, COALESCE(tm.decimals, 18))
                    ELSE 0
                END
            ) as usd_volume
        FROM aave_ethereum.LiquidationCall l
        LEFT JOIN aave_ethereum.view_LatestAssetConfiguration tm
            ON l.collateralAsset = tm.asset
        LEFT JOIN aave_ethereum.LatestPriceEvent lpe
            ON l.collateralAsset = lpe.asset
        WHERE l.blockTimestamp >= now() - INTERVAL {interval}
        {min_value_condition}
        GROUP BY l.liquidator
        ORDER BY usd_volume DESC
        LIMIT 20
        """

        result = clickhouse_client.execute_query(top_liquidators_query)

        data = []
        for row in result.result_rows:
            data.append(
                {
                    "liquidator": row[0],
                    "txn_count": int(row[1]) if row[1] else 0,
                    "usd_volume": float(row[2]) if row[2] else 0.0,
                    "liquidator_url": get_simple_explorer_url(row[0])
                    if row[0]
                    else None,
                }
            )

        return JsonResponse({"data": data})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def liquidations_timeseries(request):
    """API endpoint to get liquidations timeseries data"""
    try:
        time_window = request.GET.get("time_window", "1_day")
        min_value = request.GET.get("min_value")

        # Convert time window to ClickHouse interval
        interval_map = {
            "1_hour": "1 HOUR",
            "1_day": "1 DAY",
            "1_week": "7 DAY",
            "1_month": "30 DAY",
            "1_year": "365 DAY",
        }

        interval = interval_map.get(time_window, "1 DAY")

        # Add min_value filter condition if specified
        min_value_condition = ""
        if min_value:
            try:
                min_val = float(min_value)
                min_value_condition = f"""
                AND (
                    CASE
                        WHEN lpe.historical_price_usd > 0 AND COALESCE(tm.decimals, 18) > 0
                        THEN (toFloat64(l.liquidatedCollateralAmount) * lpe.historical_price_usd) / POW(10, COALESCE(tm.decimals, 18))
                        ELSE 0
                    END
                ) >= {min_val}
                """
            except ValueError:
                pass

        # Determine time grouping based on interval
        if time_window == "1_hour":
            time_format = (
                "toDateTime(toStartOfInterval(l.blockTimestamp, INTERVAL 5 MINUTE))"
            )
        elif time_window == "1_day":
            time_format = (
                "toDateTime(toStartOfInterval(l.blockTimestamp, INTERVAL 1 HOUR))"
            )
        elif time_window == "1_week":
            time_format = (
                "toDateTime(toStartOfInterval(l.blockTimestamp, INTERVAL 6 HOUR))"
            )
        elif time_window == "1_month":
            time_format = (
                "toDateTime(toStartOfInterval(l.blockTimestamp, INTERVAL 1 DAY))"
            )
        else:  # 1_year
            time_format = (
                "toDateTime(toStartOfInterval(l.blockTimestamp, INTERVAL 1 WEEK))"
            )

        # Query for timeseries data
        timeseries_query = f"""
        SELECT
            {time_format} as time_bucket,
            COUNT(*) as liquidation_count,
            SUM(
                CASE
                    WHEN lpe.historical_price_usd > 0 AND COALESCE(tm.decimals, 18) > 0
                    THEN (toFloat64(l.liquidatedCollateralAmount) * lpe.historical_price_usd) / POW(10, COALESCE(tm.decimals, 18))
                    ELSE 0
                END
            ) as usd_volume
        FROM aave_ethereum.LiquidationCall l
        LEFT JOIN aave_ethereum.view_LatestAssetConfiguration tm
            ON l.collateralAsset = tm.asset
        LEFT JOIN aave_ethereum.LatestPriceEvent lpe
            ON l.collateralAsset = lpe.asset
        WHERE l.blockTimestamp >= now() - INTERVAL {interval}
        {min_value_condition}
        GROUP BY time_bucket
        ORDER BY time_bucket ASC
        """

        result = clickhouse_client.execute_query(timeseries_query)

        data = []
        for row in result.result_rows:
            data.append(
                {
                    "timestamp": row[0].isoformat() if row[0] else None,
                    "liquidation_count": int(row[1]) if row[1] else 0,
                    "usd_volume": float(row[2]) if row[2] else 0.0,
                }
            )

        return JsonResponse({"data": data})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def liquidations_recent(request):
    """API endpoint to get recent liquidations with pagination"""
    try:
        time_window = request.GET.get("time_window", "1_day")
        page = int(request.GET.get("page", 1))
        page_size = int(request.GET.get("page_size", 20))
        min_value = request.GET.get("min_value")

        # Convert time window to ClickHouse interval
        interval_map = {
            "1_hour": "1 HOUR",
            "1_day": "1 DAY",
            "1_week": "7 DAY",
            "1_month": "30 DAY",
            "1_year": "365 DAY",
        }

        interval = interval_map.get(time_window, "1 DAY")

        # Add min_value filter condition if specified
        min_value_condition = ""
        if min_value:
            try:
                min_val = float(min_value)
                min_value_condition = f"""
                AND (
                    CASE
                        WHEN lpe.historical_price_usd > 0 AND COALESCE(collateral_meta.decimals, 18) > 0
                        THEN (toFloat64(l.liquidatedCollateralAmount) * lpe.historical_price_usd) / POW(10, COALESCE(collateral_meta.decimals, 18))
                        ELSE 0
                    END
                ) >= {min_val}
                """
            except ValueError:
                pass

        # Calculate offset for pagination
        offset = (page - 1) * page_size

        # First, get total count for pagination
        count_query = f"""
        SELECT COUNT(*) as total_count
        FROM aave_ethereum.LiquidationCall l
        LEFT JOIN aave_ethereum.view_LatestAssetConfiguration collateral_meta
            ON l.collateralAsset = collateral_meta.asset
        LEFT JOIN aave_ethereum.LatestPriceEvent lpe
            ON l.collateralAsset = lpe.asset
        WHERE l.blockTimestamp >= now() - INTERVAL {interval}
        {min_value_condition}
        """

        count_result = clickhouse_client.execute_query(count_query)
        total_count = (
            int(count_result.result_rows[0][0]) if count_result.result_rows else 0
        )

        # Query for recent liquidations with pagination
        recent_query = f"""
        SELECT
            l.collateralAsset,
            l.debtAsset,
            toFloat64(l.liquidatedCollateralAmount) as liquidatedCollateralAmount,
            l.liquidator,
            l.user,
            l.blockTimestamp,
            l.transactionHash,
            l.blockNumber,
            COALESCE(toFloat64(collateral_meta.decimals), 18.0) as collateral_decimals,
            COALESCE(lpe.historical_price_usd, 0.0) as historical_price_usd,
            collateral_meta.symbol as collateral_symbol,
            debt_meta.symbol as debt_symbol,
            CASE
                WHEN lpe.historical_price_usd > 0 AND COALESCE(collateral_meta.decimals, 18) > 0
                THEN (toFloat64(l.liquidatedCollateralAmount) * lpe.historical_price_usd) / POW(10, COALESCE(collateral_meta.decimals, 18))
                ELSE 0
            END as usd_volume,
            hf.health_factor_at_transaction,
            hf.health_factor_at_previous_tx,
            hf.health_factor_at_block_start,
            hf.health_factor_at_previous_block,
            hf.health_factor_at_two_blocks_prior
        FROM aave_ethereum.LiquidationCall l
        LEFT JOIN aave_ethereum.view_LatestAssetConfiguration collateral_meta
            ON l.collateralAsset = collateral_meta.asset
        LEFT JOIN aave_ethereum.view_LatestAssetConfiguration debt_meta
            ON l.debtAsset = debt_meta.asset
        LEFT JOIN aave_ethereum.LatestPriceEvent lpe
            ON l.collateralAsset = lpe.asset
        LEFT JOIN aave_ethereum.LiquidationHealthFactorMetrics hf
            ON l.transactionHash = hf.transaction_hash
            AND l.logIndex = hf.log_index
        WHERE l.blockTimestamp >= now() - INTERVAL {interval}
        {min_value_condition}
        ORDER BY l.blockTimestamp DESC
        LIMIT {page_size} OFFSET {offset}
        """

        result = clickhouse_client.execute_query(recent_query)

        data = []
        for row in result.result_rows:
            liquidation = {
                "collateralAsset": row[0],
                "debtAsset": row[1],
                "liquidatedCollateralAmount": float(row[2]) if row[2] else 0,
                "liquidator": row[3],
                "user": row[4],
                "blockTimestamp": row[5].isoformat() if row[5] else None,
                "transactionHash": row[6],
                "blockNumber": int(row[7]) if row[7] else 0,
                "collateral_decimals": float(row[8]) if row[8] else 18.0,
                "historical_price_usd": float(row[9]) if row[9] else 0.0,
                "collateral_symbol": row[10],
                "debt_symbol": row[11],
                "usd_volume": float(row[12]) if row[12] else 0.0,
                "health_factor_at_transaction": float(row[13])
                if row[13] is not None
                else None,
                "health_factor_at_previous_tx": float(row[14])
                if row[14] is not None
                else None,
                "health_factor_at_block_start": float(row[15])
                if row[15] is not None
                else None,
                "health_factor_at_previous_block": float(row[16])
                if row[16] is not None
                else None,
                "health_factor_at_two_blocks_prior": float(row[17])
                if row[17] is not None
                else None,
                "liquidator_url": get_simple_explorer_url(row[3]) if row[3] else None,
                "user_url": get_simple_explorer_url(row[4]) if row[4] else None,
                "transaction_url": get_simple_transaction_url(row[6])
                if row[6]
                else None,
            }
            data.append(liquidation)

        # Calculate pagination info
        total_pages = (total_count + page_size - 1) // page_size
        has_previous = page > 1
        has_next = page < total_pages
        start_item = offset + 1 if total_count > 0 else 0
        end_item = min(offset + page_size, total_count)

        pagination = {
            "current_page": page,
            "total_pages": total_pages,
            "total_items": total_count,
            "page_size": page_size,
            "has_previous": has_previous,
            "has_next": has_next,
            "start_item": start_item,
            "end_item": end_item,
        }

        return JsonResponse({"data": data, "pagination": pagination})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def liquidation_detail(request, transaction_hash):
    """View to show detailed liquidation information including RPC data"""
    try:
        # Get liquidation data from ClickHouse
        liquidation_query = f"""
        SELECT
            l.collateralAsset,
            l.debtAsset,
            toFloat64(l.liquidatedCollateralAmount) as liquidatedCollateralAmount,
            toFloat64(l.debtToCover) as debtToCover,
            l.liquidator,
            l.user,
            l.blockTimestamp,
            l.transactionHash,
            l.blockNumber,
            l.transactionIndex,
            l.logIndex,
            COALESCE(toFloat64(collateral_meta.decimals), 18.0) as collateral_decimals,
            COALESCE(toFloat64(debt_meta.decimals), 18.0) as debt_decimals,
            COALESCE(collateral_lpe.historical_price_usd, 0.0) as collateral_price_usd,
            COALESCE(debt_lpe.historical_price_usd, 0.0) as debt_price_usd,
            collateral_meta.symbol as collateral_symbol,
            debt_meta.symbol as debt_symbol,
            collateral_meta.name as collateral_name,
            debt_meta.name as debt_name
        FROM aave_ethereum.LiquidationCall l
        LEFT JOIN aave_ethereum.view_LatestAssetConfiguration collateral_meta
            ON l.collateralAsset = collateral_meta.asset
        LEFT JOIN aave_ethereum.view_LatestAssetConfiguration debt_meta
            ON l.debtAsset = debt_meta.asset
        LEFT JOIN aave_ethereum.LatestPriceEvent collateral_lpe
            ON l.collateralAsset = collateral_lpe.asset
        LEFT JOIN aave_ethereum.LatestPriceEvent debt_lpe
            ON l.debtAsset = debt_lpe.asset
        WHERE l.transactionHash = '{transaction_hash}'
        LIMIT 1
        """

        liquidation_result = clickhouse_client.execute_query(liquidation_query)

        if not liquidation_result.result_rows:
            return render(
                request,
                "dashboard/liquidation_detail.html",
                {"error": "Liquidation transaction not found"},
            )

        row = liquidation_result.result_rows[0]
        liquidation_data = {
            "collateralAsset": row[0],
            "debtAsset": row[1],
            "liquidatedCollateralAmount": float(row[2]) if row[2] else 0,
            "debtToCover": float(row[3]) if row[3] else 0,
            "liquidator": row[4],
            "user": row[5],
            "blockTimestamp": row[6],
            "transactionHash": row[7],
            "blockNumber": int(row[8]) if row[8] else 0,
            "transactionIndex": int(row[9]) if row[9] else 0,
            "logIndex": int(row[10]) if row[10] else 0,
            "collateral_decimals": float(row[11]) if row[11] else 18.0,
            "debt_decimals": float(row[12]) if row[12] else 18.0,
            "collateral_price_usd": float(row[13]) if row[13] else 0.0,
            "debt_price_usd": float(row[14]) if row[14] else 0.0,
            "collateral_symbol": row[15],
            "debt_symbol": row[16],
            "collateral_name": row[17],
            "debt_name": row[18],
        }

        # Calculate USD values
        liquidation_data["collateral_usd_value"] = (
            liquidation_data["liquidatedCollateralAmount"]
            * liquidation_data["collateral_price_usd"]
            / (10 ** liquidation_data["collateral_decimals"])
        )

        liquidation_data["debt_usd_value"] = (
            liquidation_data["debtToCover"]
            * liquidation_data["debt_price_usd"]
            / (10 ** liquidation_data["debt_decimals"])
        )

        # Add explorer URLs
        liquidation_data["liquidator_url"] = get_simple_explorer_url(
            liquidation_data["liquidator"]
        )
        liquidation_data["user_url"] = get_simple_explorer_url(liquidation_data["user"])
        liquidation_data["transaction_url"] = get_simple_transaction_url(
            liquidation_data["transactionHash"]
        )
        liquidation_data["collateral_asset_url"] = get_simple_explorer_url(
            liquidation_data["collateralAsset"]
        )
        liquidation_data["debt_asset_url"] = get_simple_explorer_url(
            liquidation_data["debtAsset"]
        )

        # Get health factor at 5 different time points
        health_factors = []
        user_address = liquidation_data["user"]
        block_number = liquidation_data["blockNumber"]
        transaction_index = liquidation_data["transactionIndex"]

        # Define the 5 time points to check
        time_points = [
            {
                "name": "At Liquidation Transaction",
                "block": block_number,
                "txn_index": transaction_index,
                "description": f"Block {block_number}, Transaction Index {transaction_index}",
            },
            {
                "name": "Previous Transaction",
                "block": block_number,
                "txn_index": max(0, transaction_index - 1),
                "description": f"Block {block_number}, Transaction Index {max(0, transaction_index - 1)}",
            },
            {
                "name": "Start of Block",
                "block": block_number,
                "txn_index": 0,
                "description": f"Block {block_number}, Transaction Index 0",
            },
            {
                "name": "Previous Block",
                "block": max(0, block_number - 1),
                "txn_index": 0,
                "description": f"Block {max(0, block_number - 1)}, Transaction Index 0",
            },
            {
                "name": "Two Blocks Ago",
                "block": max(0, block_number - 2),
                "txn_index": 0,
                "description": f"Block {max(0, block_number - 2)}, Transaction Index 0",
            },
        ]

        for point in time_points:
            try:
                health_factor = get_simulated_health_factor(
                    chain_id=NETWORK_ID,
                    block_number=point["block"],
                    address=user_address,
                    transaction_index=point["txn_index"],
                )
                health_factors.append(
                    {
                        "name": point["name"],
                        "description": point["description"],
                        "health_factor": health_factor,
                        "formatted_health_factor": f"{health_factor:.6f}"
                        if health_factor
                        else "N/A",
                        "block": point["block"],
                        "txn_index": point["txn_index"],
                        "error": None,
                    }
                )
            except Exception as hf_error:
                health_factors.append(
                    {
                        "name": point["name"],
                        "description": point["description"],
                        "health_factor": None,
                        "formatted_health_factor": "Error",
                        "block": point["block"],
                        "txn_index": point["txn_index"],
                        "error": str(hf_error),
                    }
                )

        # Get RPC transaction data
        rpc_data = {}
        try:
            transaction_data = rpc_adapter.get_raw_transaction(transaction_hash)
            receipt_data = rpc_adapter.get_transaction_receipt(transaction_hash)

            # Format transaction data
            rpc_data["transaction"] = {
                "hash": transaction_data.hash.hex()
                if hasattr(transaction_data.hash, "hex")
                else str(transaction_data.hash),
                "blockNumber": transaction_data.blockNumber,
                "blockHash": transaction_data.blockHash.hex()
                if hasattr(transaction_data.blockHash, "hex")
                else str(transaction_data.blockHash),
                "transactionIndex": transaction_data.transactionIndex,
                "from": transaction_data.get("from", ""),
                "to": transaction_data.get("to", ""),
                "value": str(transaction_data.value),
                "gas": transaction_data.gas,
                "gasPrice": str(transaction_data.gasPrice),
                "input": transaction_data.input.hex()
                if hasattr(transaction_data.input, "hex")
                else str(transaction_data.input),
                "nonce": transaction_data.nonce,
            }

            # Format receipt data
            rpc_data["receipt"] = {
                "status": receipt_data.status,
                "gasUsed": receipt_data.gasUsed,
                "effectiveGasPrice": str(receipt_data.get("effectiveGasPrice", 0)),
                "cumulativeGasUsed": receipt_data.cumulativeGasUsed,
                "logsBloom": receipt_data.logsBloom.hex()
                if hasattr(receipt_data.logsBloom, "hex")
                else str(receipt_data.logsBloom),
            }

            # Parse logs/events
            rpc_data["logs"] = []
            raw_logs = []
            for log in receipt_data.logs:
                log_data = {
                    "address": log.address,
                    "topics": [
                        topic.hex() if hasattr(topic, "hex") else str(topic)
                        for topic in log.topics
                    ],
                    "data": log.data.hex()
                    if hasattr(log.data, "hex")
                    else str(log.data),
                    "blockNumber": log.blockNumber,
                    "transactionHash": log.transactionHash.hex()
                    if hasattr(log.transactionHash, "hex")
                    else str(log.transactionHash),
                    "transactionIndex": log.transactionIndex,
                    "blockHash": log.blockHash.hex()
                    if hasattr(log.blockHash, "hex")
                    else str(log.blockHash),
                    "logIndex": log.logIndex,
                    "removed": log.removed,
                }
                rpc_data["logs"].append(log_data)
                raw_logs.append(log_data)

            # Parse logs using Aave ABI
            try:
                rpc_data["parsed_logs"] = parse_transaction_logs(raw_logs)
            except Exception as parse_error:
                rpc_data["parsed_logs"] = []
                rpc_data["parse_error"] = str(parse_error)

        except Exception as rpc_error:
            rpc_data["error"] = f"Failed to fetch RPC data: {str(rpc_error)}"

        # Get previous transaction data (same block, transaction index - 1)
        prev_transaction_data = {}
        try:
            block_number = liquidation_data.get("blockNumber")
            transaction_index = liquidation_data.get("transactionIndex")

            if block_number and transaction_index is not None and transaction_index > 0:
                # Fetch previous transaction
                prev_txn_index = transaction_index - 1
                prev_transaction = rpc_adapter.get_transaction_by_block_and_index(
                    block_number, prev_txn_index
                )

                if prev_transaction:
                    # Get transaction receipt for logs
                    prev_txn_hash = (
                        prev_transaction.hash.hex()
                        if hasattr(prev_transaction.hash, "hex")
                        else str(prev_transaction.hash)
                    )
                    prev_receipt = rpc_adapter.get_transaction_receipt(prev_txn_hash)

                    # Format previous transaction data
                    prev_transaction_data = {
                        "transaction": {
                            "hash": prev_txn_hash,
                            "blockNumber": prev_transaction.blockNumber,
                            "blockHash": prev_transaction.blockHash.hex()
                            if hasattr(prev_transaction.blockHash, "hex")
                            else str(prev_transaction.blockHash),
                            "transactionIndex": prev_transaction.transactionIndex,
                            "from": prev_transaction.get("from", ""),
                            "to": prev_transaction.get("to", ""),
                            "value": str(prev_transaction.value),
                            "gas": prev_transaction.gas,
                            "gasPrice": str(prev_transaction.gasPrice),
                            "input": prev_transaction.input.hex()
                            if hasattr(prev_transaction.input, "hex")
                            else str(prev_transaction.input),
                            "nonce": prev_transaction.nonce,
                        },
                        "receipt": {
                            "status": prev_receipt.status,
                            "gasUsed": prev_receipt.gasUsed,
                            "effectiveGasPrice": str(
                                prev_receipt.get("effectiveGasPrice", 0)
                            ),
                            "cumulativeGasUsed": prev_receipt.cumulativeGasUsed,
                        },
                        "logs": [],
                    }

                    # Parse logs for previous transaction
                    raw_logs = []
                    for log in prev_receipt.logs:
                        log_data = {
                            "address": log.address,
                            "topics": [
                                topic.hex() if hasattr(topic, "hex") else str(topic)
                                for topic in log.topics
                            ],
                            "data": log.data.hex()
                            if hasattr(log.data, "hex")
                            else str(log.data),
                            "blockNumber": log.blockNumber,
                            "transactionHash": log.transactionHash.hex()
                            if hasattr(log.transactionHash, "hex")
                            else str(log.transactionHash),
                            "transactionIndex": log.transactionIndex,
                            "blockHash": log.blockHash.hex()
                            if hasattr(log.blockHash, "hex")
                            else str(log.blockHash),
                            "logIndex": log.logIndex,
                            "removed": log.removed,
                        }
                        prev_transaction_data["logs"].append(log_data)
                        raw_logs.append(log_data)

                    # Parse logs using Aave ABI
                    try:
                        prev_transaction_data["parsed_logs"] = parse_transaction_logs(
                            raw_logs
                        )
                    except Exception as parse_error:
                        prev_transaction_data["parsed_logs"] = []
                        prev_transaction_data["parse_error"] = str(parse_error)

                else:
                    prev_transaction_data["error"] = (
                        f"Previous transaction not found at block {block_number}, index {prev_txn_index}"
                    )
            else:
                prev_transaction_data["error"] = (
                    "No previous transaction available (transaction index is 0 or invalid data)"
                )

        except Exception as prev_error:
            prev_transaction_data["error"] = (
                f"Failed to fetch previous transaction data: {str(prev_error)}"
            )

        context = {
            "liquidation": liquidation_data,
            "rpc": rpc_data,
            "prev_transaction": prev_transaction_data,
            "health_factors": health_factors,
            "transaction_hash": transaction_hash,
        }

        return render(request, "dashboard/liquidation_detail.html", context)

    except Exception as e:
        return render(
            request,
            "dashboard/liquidation_detail.html",
            {"error": f"Error loading liquidation details: {str(e)}"},
        )


@login_required
def liquidator_detail(request, liquidator_address):
    """View to show liquidations performed by a specific liquidator"""
    try:
        # Get page parameters
        page = int(request.GET.get("page", 1))
        page_size = int(request.GET.get("page_size", 20))
        time_window = request.GET.get("time_window", "1_month")
        min_value = request.GET.get(
            "min_value", "1000"
        )  # Default to 1000 for enabled by default

        # Calculate offset
        offset = (page - 1) * page_size

        # Time window mapping
        time_intervals = {
            "1_hour": "INTERVAL 1 HOUR",
            "1_day": "INTERVAL 1 DAY",
            "1_week": "INTERVAL 1 WEEK",
            "1_month": "INTERVAL 1 MONTH",
            "1_year": "INTERVAL 1 YEAR",
        }

        time_filter = time_intervals.get(time_window, "INTERVAL 1 MONTH")

        # Add min_value filter condition if specified
        min_value_condition = ""
        if min_value:
            try:
                min_val = float(min_value)
                min_value_condition = f"""
                AND ((toFloat64(l.liquidatedCollateralAmount) * COALESCE(cp.historical_price_usd, 0)) / POW(10, COALESCE(ca.decimals, 18))) >= {min_val}
                """
            except ValueError:
                pass

        # Get liquidator summary stats
        summary_query = f"""
        SELECT
            COUNT(*) as total_liquidations,
            SUM((toFloat64(l.liquidatedCollateralAmount) * COALESCE(cp.historical_price_usd, 0)) / POW(10, COALESCE(ca.decimals, 18))) as total_usd_volume,
            MIN(l.blockTimestamp) as first_liquidation,
            MAX(l.blockTimestamp) as last_liquidation
        FROM aave_ethereum.LiquidationCall l
        LEFT JOIN aave_ethereum.LatestPriceEvent cp ON l.collateralAsset = cp.asset
        LEFT JOIN aave_ethereum.view_LatestAssetConfiguration ca ON l.collateralAsset = ca.asset
        WHERE l.liquidator = '{liquidator_address}'
        AND l.blockTimestamp >= now() - {time_filter}
        {min_value_condition}
        """

        summary_result = clickhouse_client.execute_query(summary_query)
        summary_data = {}
        if summary_result.result_rows:
            row = summary_result.result_rows[0]
            summary_data = {
                "total_liquidations": row[0] or 0,
                "total_usd_volume": row[1] or 0,
                "first_liquidation": row[2],
                "last_liquidation": row[3],
                "liquidator_address": liquidator_address,
                "liquidator_url": get_simple_explorer_url(liquidator_address),
            }

        # Get count for pagination
        count_query = f"""
        SELECT COUNT(*) as total
        FROM aave_ethereum.LiquidationCall l
        LEFT JOIN aave_ethereum.LatestPriceEvent cp ON l.collateralAsset = cp.asset
        LEFT JOIN aave_ethereum.view_LatestAssetConfiguration ca ON l.collateralAsset = ca.asset
        WHERE l.liquidator = '{liquidator_address}'
        AND l.blockTimestamp >= now() - {time_filter}
        {min_value_condition}
        """

        count_result = clickhouse_client.execute_query(count_query)
        total_items = count_result.result_rows[0][0] if count_result.result_rows else 0

        # Get liquidations list
        liquidations_query = f"""
        SELECT
            l.transactionHash,
            l.blockTimestamp,
            l.blockNumber,
            l.transactionIndex,
            l.logIndex,
            l.liquidator,
            l.user,
            l.collateralAsset,
            l.debtAsset,
            toFloat64(l.liquidatedCollateralAmount) as liquidatedCollateralAmount,
            toFloat64(l.debtToCover) as debtToCover,
            COALESCE(ca.symbol, 'Unknown') as collateral_symbol,
            COALESCE(ca.name, 'Unknown') as collateral_name,
            COALESCE(ca.decimals, 18) as collateral_decimals,
            COALESCE(da.symbol, 'Unknown') as debt_symbol,
            COALESCE(da.name, 'Unknown') as debt_name,
            COALESCE(da.decimals, 18) as debt_decimals,
            COALESCE(cp.historical_price_usd, 0) as collateral_price,
            COALESCE(dp.historical_price_usd, 0) as debt_price,
            hf.health_factor_at_transaction,
            hf.health_factor_at_previous_tx,
            hf.health_factor_at_block_start,
            hf.health_factor_at_previous_block,
            hf.health_factor_at_two_blocks_prior
        FROM aave_ethereum.LiquidationCall l
        LEFT JOIN aave_ethereum.view_LatestAssetConfiguration ca ON l.collateralAsset = ca.asset
        LEFT JOIN aave_ethereum.view_LatestAssetConfiguration da ON l.debtAsset = da.asset
        LEFT JOIN aave_ethereum.LatestPriceEvent cp ON l.collateralAsset = cp.asset
        LEFT JOIN aave_ethereum.LatestPriceEvent dp ON l.debtAsset = dp.asset
        LEFT JOIN aave_ethereum.LiquidationHealthFactorMetrics hf
            ON l.transactionHash = hf.transaction_hash
            AND l.logIndex = hf.log_index
        WHERE l.liquidator = '{liquidator_address}'
        AND l.blockTimestamp >= now() - {time_filter}
        {min_value_condition}
        ORDER BY l.blockTimestamp DESC
        LIMIT {page_size} OFFSET {offset}
        """

        liquidations_result = clickhouse_client.execute_query(liquidations_query)
        liquidations = []

        for row in liquidations_result.result_rows:
            # Calculate USD values - note: indices shifted due to health factor columns
            collateral_decimals = row[13] or 18
            debt_decimals = row[16] or 18
            collateral_price = row[17] or 0
            debt_price = row[18] or 0

            collateral_usd_value = (
                (float(row[9]) * collateral_price) / (10**collateral_decimals)
                if row[9]
                else 0
            )
            debt_usd_value = (
                (float(row[10]) * debt_price) / (10**debt_decimals) if row[10] else 0
            )

            liquidation_data = {
                "transactionHash": row[0],
                "blockTimestamp": row[1],
                "blockNumber": row[2],
                "transactionIndex": row[3],
                "logIndex": row[4],
                "liquidator": row[5],
                "user": row[6],
                "collateralAsset": row[7],
                "debtAsset": row[8],
                "liquidatedCollateralAmount": row[9],
                "debtToCover": row[10],
                "collateral_symbol": row[11] or "Unknown",
                "collateral_name": row[12] or "Unknown",
                "debt_symbol": row[14] or "Unknown",
                "debt_name": row[15] or "Unknown",
                "collateral_usd_value": collateral_usd_value,
                "debt_usd_value": debt_usd_value,
                "usd_volume": collateral_usd_value,  # Use collateral USD value as volume
                "health_factor_at_transaction": float(row[19])
                if row[19] is not None
                else None,
                "health_factor_at_previous_tx": float(row[20])
                if row[20] is not None
                else None,
                "health_factor_at_block_start": float(row[21])
                if row[21] is not None
                else None,
                "health_factor_at_previous_block": float(row[22])
                if row[22] is not None
                else None,
                "health_factor_at_two_blocks_prior": float(row[23])
                if row[23] is not None
                else None,
                "transaction_url": get_simple_transaction_url(row[0]),
                "liquidator_url": get_simple_explorer_url(row[5]),
                "user_url": get_simple_explorer_url(row[6]),
                "collateral_asset_url": get_simple_explorer_url(row[7]),
                "debt_asset_url": get_simple_explorer_url(row[8]),
            }
            liquidations.append(liquidation_data)

        # Calculate pagination info
        has_next = offset + page_size < total_items
        has_previous = page > 1
        start_item = offset + 1 if liquidations else 0
        end_item = min(offset + len(liquidations), total_items)

        pagination = {
            "current_page": page,
            "total_pages": (total_items + page_size - 1) // page_size,
            "has_next": has_next,
            "has_previous": has_previous,
            "start_item": start_item,
            "end_item": end_item,
            "total_items": total_items,
        }

        context = {
            "summary": summary_data,
            "liquidations": liquidations,
            "pagination": pagination,
            "time_window": time_window,
            "liquidator_address": liquidator_address,
        }

        return render(request, "dashboard/liquidator_detail.html", context)

    except Exception as e:
        return render(
            request,
            "dashboard/liquidator_detail.html",
            {
                "error": f"Error loading liquidator details: {str(e)}",
                "liquidator_address": liquidator_address,
            },
        )


def health_factor_analytics(request):
    """API endpoint to get health factor analytics and liquidation classification"""
    try:
        time_window = request.GET.get("time_window", "1_month")

        # Convert time window to ClickHouse interval
        interval_map = {
            "1_hour": "1 HOUR",
            "1_day": "1 DAY",
            "1_week": "7 DAY",
            "1_month": "30 DAY",
            "1_year": "365 DAY",
        }

        interval = interval_map.get(time_window, "30 DAY")

        # Query to get liquidation classification and collateral size distribution
        analytics_query = f"""
        WITH liquidation_data AS (
            SELECT
                hf.health_factor_at_transaction,
                hf.health_factor_at_previous_tx,
                hf.health_factor_at_block_start,
                hf.health_factor_at_previous_block,
                hf.health_factor_at_two_blocks_prior,
                CASE
                    WHEN lpe.historical_price_usd > 0 AND COALESCE(collateral_meta.decimals, 18) > 0
                    THEN (toFloat64(l.liquidatedCollateralAmount) * lpe.historical_price_usd) / POW(10, COALESCE(collateral_meta.decimals, 18))
                    ELSE 0
                END as usd_volume,
                -- Classification logic
                CASE
                    -- Ultra Fast: HF at transaction < 1, but all others >= 1
                    WHEN hf.health_factor_at_transaction < 1
                         AND COALESCE(hf.health_factor_at_previous_tx, 1) >= 1
                         AND COALESCE(hf.health_factor_at_block_start, 1) >= 1
                         AND COALESCE(hf.health_factor_at_previous_block, 1) >= 1
                         AND COALESCE(hf.health_factor_at_two_blocks_prior, 1) >= 1
                    THEN 'ultra_fast'
                    -- Fast: HF at txn, prev txn, and block start < 1, but others >= 1
                    WHEN hf.health_factor_at_transaction < 1
                         AND COALESCE(hf.health_factor_at_previous_tx, 1) < 1
                         AND COALESCE(hf.health_factor_at_block_start, 1) < 1
                         AND COALESCE(hf.health_factor_at_previous_block, 1) >= 1
                         AND COALESCE(hf.health_factor_at_two_blocks_prior, 1) >= 1
                    THEN 'fast'
                    -- Slow: HF at previous block < 1
                    WHEN COALESCE(hf.health_factor_at_previous_block, 1) < 1
                    THEN 'slow'
                    ELSE 'slow'
                END as liquidation_speed,
                -- Size buckets based on usd_volume
                CASE
                    WHEN CASE
                        WHEN lpe.historical_price_usd > 0 AND COALESCE(collateral_meta.decimals, 18) > 0
                        THEN (toFloat64(l.liquidatedCollateralAmount) * lpe.historical_price_usd) / POW(10, COALESCE(collateral_meta.decimals, 18))
                        ELSE 0
                    END < 100 THEN 'under_100'
                    WHEN CASE
                        WHEN lpe.historical_price_usd > 0 AND COALESCE(collateral_meta.decimals, 18) > 0
                        THEN (toFloat64(l.liquidatedCollateralAmount) * lpe.historical_price_usd) / POW(10, COALESCE(collateral_meta.decimals, 18))
                        ELSE 0
                    END < 1000 THEN 'under_1000'
                    WHEN CASE
                        WHEN lpe.historical_price_usd > 0 AND COALESCE(collateral_meta.decimals, 18) > 0
                        THEN (toFloat64(l.liquidatedCollateralAmount) * lpe.historical_price_usd) / POW(10, COALESCE(collateral_meta.decimals, 18))
                        ELSE 0
                    END < 10000 THEN 'under_10000'
                    ELSE 'over_10000'
                END as size_bucket
            FROM aave_ethereum.LiquidationCall l
            INNER JOIN aave_ethereum.LiquidationHealthFactorMetrics hf
                ON l.transactionHash = hf.transaction_hash
                AND l.logIndex = hf.log_index
            LEFT JOIN aave_ethereum.view_LatestAssetConfiguration collateral_meta
                ON l.collateralAsset = collateral_meta.asset
            LEFT JOIN aave_ethereum.LatestPriceEvent lpe
                ON l.collateralAsset = lpe.asset
            WHERE l.blockTimestamp >= now() - INTERVAL {interval}
        )
        SELECT
            -- Category counts (indices 0-3)
            COUNT(*) as total_with_hf,
            SUM(CASE WHEN liquidation_speed = 'ultra_fast' THEN 1 ELSE 0 END) as ultra_fast_count,
            SUM(CASE WHEN liquidation_speed = 'fast' THEN 1 ELSE 0 END) as fast_count,
            SUM(CASE WHEN liquidation_speed = 'slow' THEN 1 ELSE 0 END) as slow_count,

            -- Size distribution by category - Ultra Fast (indices 4-7)
            SUM(CASE WHEN liquidation_speed = 'ultra_fast' AND size_bucket = 'under_100' THEN 1 ELSE 0 END) as ultra_fast_under_100,
            SUM(CASE WHEN liquidation_speed = 'ultra_fast' AND size_bucket = 'under_1000' THEN 1 ELSE 0 END) as ultra_fast_under_1000,
            SUM(CASE WHEN liquidation_speed = 'ultra_fast' AND size_bucket = 'under_10000' THEN 1 ELSE 0 END) as ultra_fast_under_10000,
            SUM(CASE WHEN liquidation_speed = 'ultra_fast' AND size_bucket = 'over_10000' THEN 1 ELSE 0 END) as ultra_fast_over_10000,

            -- Size distribution by category - Fast (indices 8-11)
            SUM(CASE WHEN liquidation_speed = 'fast' AND size_bucket = 'under_100' THEN 1 ELSE 0 END) as fast_under_100,
            SUM(CASE WHEN liquidation_speed = 'fast' AND size_bucket = 'under_1000' THEN 1 ELSE 0 END) as fast_under_1000,
            SUM(CASE WHEN liquidation_speed = 'fast' AND size_bucket = 'under_10000' THEN 1 ELSE 0 END) as fast_under_10000,
            SUM(CASE WHEN liquidation_speed = 'fast' AND size_bucket = 'over_10000' THEN 1 ELSE 0 END) as fast_over_10000,

            -- Size distribution by category - Slow (indices 12-15)
            SUM(CASE WHEN liquidation_speed = 'slow' AND size_bucket = 'under_100' THEN 1 ELSE 0 END) as slow_under_100,
            SUM(CASE WHEN liquidation_speed = 'slow' AND size_bucket = 'under_1000' THEN 1 ELSE 0 END) as slow_under_1000,
            SUM(CASE WHEN liquidation_speed = 'slow' AND size_bucket = 'under_10000' THEN 1 ELSE 0 END) as slow_under_10000,
            SUM(CASE WHEN liquidation_speed = 'slow' AND size_bucket = 'over_10000' THEN 1 ELSE 0 END) as slow_over_10000,

            -- Total all liquidations (index 16)
            (SELECT COUNT(*) FROM aave_ethereum.LiquidationCall WHERE blockTimestamp >= now() - INTERVAL {interval}) as total_all_liquidations,

            -- Volume-based analytics (indices 17-19)
            SUM(CASE WHEN liquidation_speed = 'ultra_fast' THEN usd_volume ELSE 0 END) as ultra_fast_volume,
            SUM(CASE WHEN liquidation_speed = 'fast' THEN usd_volume ELSE 0 END) as fast_volume,
            SUM(CASE WHEN liquidation_speed = 'slow' THEN usd_volume ELSE 0 END) as slow_volume,

            -- Volume distribution by category and size - Ultra Fast (indices 20-23)
            SUM(CASE WHEN liquidation_speed = 'ultra_fast' AND size_bucket = 'under_100' THEN usd_volume ELSE 0 END) as ultra_fast_volume_under_100,
            SUM(CASE WHEN liquidation_speed = 'ultra_fast' AND size_bucket = 'under_1000' THEN usd_volume ELSE 0 END) as ultra_fast_volume_under_1000,
            SUM(CASE WHEN liquidation_speed = 'ultra_fast' AND size_bucket = 'under_10000' THEN usd_volume ELSE 0 END) as ultra_fast_volume_under_10000,
            SUM(CASE WHEN liquidation_speed = 'ultra_fast' AND size_bucket = 'over_10000' THEN usd_volume ELSE 0 END) as ultra_fast_volume_over_10000,

            -- Volume distribution by category and size - Fast (indices 24-27)
            SUM(CASE WHEN liquidation_speed = 'fast' AND size_bucket = 'under_100' THEN usd_volume ELSE 0 END) as fast_volume_under_100,
            SUM(CASE WHEN liquidation_speed = 'fast' AND size_bucket = 'under_1000' THEN usd_volume ELSE 0 END) as fast_volume_under_1000,
            SUM(CASE WHEN liquidation_speed = 'fast' AND size_bucket = 'under_10000' THEN usd_volume ELSE 0 END) as fast_volume_under_10000,
            SUM(CASE WHEN liquidation_speed = 'fast' AND size_bucket = 'over_10000' THEN usd_volume ELSE 0 END) as fast_volume_over_10000,

            -- Volume distribution by category and size - Slow (indices 28-31)
            SUM(CASE WHEN liquidation_speed = 'slow' AND size_bucket = 'under_100' THEN usd_volume ELSE 0 END) as slow_volume_under_100,
            SUM(CASE WHEN liquidation_speed = 'slow' AND size_bucket = 'under_1000' THEN usd_volume ELSE 0 END) as slow_volume_under_1000,
            SUM(CASE WHEN liquidation_speed = 'slow' AND size_bucket = 'under_10000' THEN usd_volume ELSE 0 END) as slow_volume_under_10000,
            SUM(CASE WHEN liquidation_speed = 'slow' AND size_bucket = 'over_10000' THEN usd_volume ELSE 0 END) as slow_volume_over_10000
        FROM liquidation_data
        """

        result = clickhouse_client.execute_query(analytics_query)

        if not result.result_rows:
            return JsonResponse(
                {
                    "total_with_hf": 0,
                    "total_all_liquidations": 0,
                    "category_distribution": {
                        "ultra_fast": {"count": 0, "percentage": 0},
                        "fast": {"count": 0, "percentage": 0},
                        "slow": {"count": 0, "percentage": 0},
                    },
                    "size_distribution": {
                        "ultra_fast": {
                            "under_100": 0,
                            "under_1000": 0,
                            "under_10000": 0,
                            "over_10000": 0,
                        },
                        "fast": {
                            "under_100": 0,
                            "under_1000": 0,
                            "under_10000": 0,
                            "over_10000": 0,
                        },
                        "slow": {
                            "under_100": 0,
                            "under_1000": 0,
                            "under_10000": 0,
                            "over_10000": 0,
                        },
                    },
                    "volume_distribution": {
                        "ultra_fast": {"total": 0, "percentage": 0},
                        "fast": {"total": 0, "percentage": 0},
                        "slow": {"total": 0, "percentage": 0},
                    },
                    "volume_size_distribution": {
                        "ultra_fast": {
                            "under_100": 0,
                            "under_1000": 0,
                            "under_10000": 0,
                            "over_10000": 0,
                        },
                        "fast": {
                            "under_100": 0,
                            "under_1000": 0,
                            "under_10000": 0,
                            "over_10000": 0,
                        },
                        "slow": {
                            "under_100": 0,
                            "under_1000": 0,
                            "under_10000": 0,
                            "over_10000": 0,
                        },
                    },
                }
            )

        row = result.result_rows[0]

        total_with_hf = row[0] or 0
        ultra_fast_count = row[1] or 0
        fast_count = row[2] or 0
        slow_count = row[3] or 0
        total_all_liquidations = row[16] or 0

        # Volume data
        ultra_fast_volume = float(row[17] or 0)
        fast_volume = float(row[18] or 0)
        slow_volume = float(row[19] or 0)
        total_volume = ultra_fast_volume + fast_volume + slow_volume

        # Calculate percentages
        def safe_percentage(part, total):
            return round((part / total * 100), 2) if total > 0 else 0

        return JsonResponse(
            {
                "total_with_hf": total_with_hf,
                "total_all_liquidations": total_all_liquidations,
                "hf_data_percentage": safe_percentage(
                    total_with_hf, total_all_liquidations
                ),
                "category_distribution": {
                    "ultra_fast": {
                        "count": ultra_fast_count,
                        "percentage": safe_percentage(ultra_fast_count, total_with_hf),
                    },
                    "fast": {
                        "count": fast_count,
                        "percentage": safe_percentage(fast_count, total_with_hf),
                    },
                    "slow": {
                        "count": slow_count,
                        "percentage": safe_percentage(slow_count, total_with_hf),
                    },
                },
                "size_distribution": {
                    "ultra_fast": {
                        "under_100": row[4] or 0,
                        "under_1000": row[5] or 0,
                        "under_10000": row[6] or 0,
                        "over_10000": row[7] or 0,
                    },
                    "fast": {
                        "under_100": row[8] or 0,
                        "under_1000": row[9] or 0,
                        "under_10000": row[10] or 0,
                        "over_10000": row[11] or 0,
                    },
                    "slow": {
                        "under_100": row[12] or 0,
                        "under_1000": row[13] or 0,
                        "under_10000": row[14] or 0,
                        "over_10000": row[15] or 0,
                    },
                },
                "volume_distribution": {
                    "ultra_fast": {
                        "total": ultra_fast_volume,
                        "percentage": safe_percentage(ultra_fast_volume, total_volume),
                    },
                    "fast": {
                        "total": fast_volume,
                        "percentage": safe_percentage(fast_volume, total_volume),
                    },
                    "slow": {
                        "total": slow_volume,
                        "percentage": safe_percentage(slow_volume, total_volume),
                    },
                },
                "volume_size_distribution": {
                    "ultra_fast": {
                        "under_100": float(row[20] or 0),
                        "under_1000": float(row[21] or 0),
                        "under_10000": float(row[22] or 0),
                        "over_10000": float(row[23] or 0),
                    },
                    "fast": {
                        "under_100": float(row[24] or 0),
                        "under_1000": float(row[25] or 0),
                        "under_10000": float(row[26] or 0),
                        "over_10000": float(row[27] or 0),
                    },
                    "slow": {
                        "under_100": float(row[28] or 0),
                        "under_1000": float(row[29] or 0),
                        "under_10000": float(row[30] or 0),
                        "over_10000": float(row[31] or 0),
                    },
                },
            }
        )

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def users(request):
    """Users page with search functionality"""
    return render(request, "dashboard/users.html")


def tests(request):
    """Tests overview page displaying all available test types"""
    try:
        # Get the latest reserve configuration test summary
        reserve_query = """
        SELECT
            test_run_id,
            test_timestamp,
            total_reserves,
            matching_records,
            mismatched_records,
            clickhouse_only_records,
            rpc_only_records,
            match_percentage,
            test_duration_seconds,
            test_status,
            error_message
        FROM aave_ethereum.ReserveConfigurationTestResults
        ORDER BY test_timestamp DESC
        LIMIT 1
        """

        reserve_result = clickhouse_client.execute_query(reserve_query)

        reserve_test_summary = None
        if reserve_result.result_rows:
            row = reserve_result.result_rows[0]
            reserve_test_summary = {
                "test_run_id": row[0],
                "test_timestamp": row[1],
                "total_reserves": row[2],
                "matching_records": row[3],
                "mismatched_records": row[4],
                "clickhouse_only_records": row[5],
                "rpc_only_records": row[6],
                "match_percentage": row[7],
                "test_duration_seconds": row[8],
                "test_status": row[9],
                "error_message": row[10],
            }

        # Get the latest user eMode test summary
        emode_query = """
        SELECT
            test_run_id,
            test_timestamp,
            total_users,
            matching_records,
            mismatched_records,
            clickhouse_only_records,
            rpc_only_records,
            match_percentage,
            test_duration_seconds,
            test_status,
            error_message
        FROM aave_ethereum.UserEModeTestResults
        ORDER BY test_timestamp DESC
        LIMIT 1
        """

        emode_result = clickhouse_client.execute_query(emode_query)

        emode_test_summary = None
        if emode_result.result_rows:
            row = emode_result.result_rows[0]
            emode_test_summary = {
                "test_run_id": row[0],
                "test_timestamp": row[1],
                "total_users": row[2],
                "matching_records": row[3],
                "mismatched_records": row[4],
                "clickhouse_only_records": row[5],
                "rpc_only_records": row[6],
                "match_percentage": row[7],
                "test_duration_seconds": row[8],
                "test_status": row[9],
                "error_message": row[10],
            }

        # Get the latest user collateral test summary
        collateral_query = """
        SELECT
            test_run_id,
            test_timestamp,
            total_user_assets,
            matching_records,
            mismatched_records,
            clickhouse_only_records,
            rpc_only_records,
            match_percentage,
            test_duration_seconds,
            test_status,
            error_message
        FROM aave_ethereum.UserCollateralTestResults
        ORDER BY test_timestamp DESC
        LIMIT 1
        """

        collateral_result = clickhouse_client.execute_query(collateral_query)

        collateral_test_summary = None
        if collateral_result.result_rows:
            row = collateral_result.result_rows[0]
            collateral_test_summary = {
                "test_run_id": row[0],
                "test_timestamp": row[1],
                "total_user_assets": row[2],
                "matching_records": row[3],
                "mismatched_records": row[4],
                "clickhouse_only_records": row[5],
                "rpc_only_records": row[6],
                "match_percentage": row[7],
                "test_duration_seconds": row[8],
                "test_status": row[9],
                "error_message": row[10],
            }

        # Get the latest collateral balance test summary
        collateral_balance_query = """
        SELECT
            test_timestamp,
            batch_offset,
            total_user_assets,
            matching_records,
            mismatched_records,
            match_percentage,
            avg_difference_bps,
            max_difference_bps,
            test_duration_seconds,
            test_status,
            error_message
        FROM aave_ethereum.CollateralBalanceTestResults
        ORDER BY test_timestamp DESC
        LIMIT 1
        """

        collateral_balance_result = clickhouse_client.execute_query(
            collateral_balance_query
        )

        collateral_balance_summary = None
        if collateral_balance_result.result_rows:
            row = collateral_balance_result.result_rows[0]
            collateral_balance_summary = {
                "test_timestamp": row[0],
                "batch_offset": row[1],
                "total_user_assets": row[2],
                "matching_records": row[3],
                "mismatched_records": row[4],
                "match_percentage": row[5],
                "avg_difference_bps": row[6],
                "max_difference_bps": row[7],
                "test_duration_seconds": row[8],
                "test_status": row[9],
                "error_message": row[10],
            }

        # Get the latest debt balance test summary
        debt_balance_query = """
        SELECT
            test_timestamp,
            batch_offset,
            total_user_assets,
            matching_records,
            mismatched_records,
            match_percentage,
            avg_difference_bps,
            max_difference_bps,
            test_duration_seconds,
            test_status,
            error_message
        FROM aave_ethereum.DebtBalanceTestResults
        ORDER BY test_timestamp DESC
        LIMIT 1
        """

        debt_balance_result = clickhouse_client.execute_query(debt_balance_query)

        debt_balance_summary = None
        if debt_balance_result.result_rows:
            row = debt_balance_result.result_rows[0]
            debt_balance_summary = {
                "test_timestamp": row[0],
                "batch_offset": row[1],
                "total_user_assets": row[2],
                "matching_records": row[3],
                "mismatched_records": row[4],
                "match_percentage": row[5],
                "avg_difference_bps": row[6],
                "max_difference_bps": row[7],
                "test_duration_seconds": row[8],
                "test_status": row[9],
                "error_message": row[10],
            }

        context = {
            "reserve_test_summary": reserve_test_summary,
            "emode_test_summary": emode_test_summary,
            "collateral_test_summary": collateral_test_summary,
            "collateral_balance_summary": collateral_balance_summary,
            "debt_balance_summary": debt_balance_summary,
        }

        return render(request, "dashboard/tests.html", context)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


def reserve_config_tests(request):
    """Reserve configuration test detail page with history"""
    try:
        # Get the latest test results
        query = """
        SELECT
            test_run_id,
            test_timestamp,
            total_reserves,
            matching_records,
            mismatched_records,
            clickhouse_only_records,
            rpc_only_records,
            match_percentage,
            test_duration_seconds,
            test_status,
            error_message,
            mismatches_detail
        FROM aave_ethereum.ReserveConfigurationTestResults
        ORDER BY test_timestamp DESC
        LIMIT 50
        """

        result = clickhouse_client.execute_query(query)

        test_results = []
        for row in result.result_rows:
            test = {
                "test_run_id": row[0],
                "test_timestamp": row[1],
                "total_reserves": row[2],
                "matching_records": row[3],
                "mismatched_records": row[4],
                "clickhouse_only_records": row[5],
                "rpc_only_records": row[6],
                "match_percentage": row[7],
                "test_duration_seconds": row[8],
                "test_status": row[9],
                "error_message": row[10],
                "mismatches_detail": row[11],
            }
            test_results.append(test)

        # Get latest test summary
        latest_test = test_results[0] if test_results else None

        context = {
            "test_results": test_results,
            "latest_test": latest_test,
        }

        return render(request, "dashboard/reserve_config_tests.html", context)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


def user_emode_tests(request):
    """User eMode test detail page with history"""
    try:
        # Get the latest test results
        query = """
        SELECT
            test_run_id,
            test_timestamp,
            total_users,
            matching_records,
            mismatched_records,
            clickhouse_only_records,
            rpc_only_records,
            match_percentage,
            test_duration_seconds,
            test_status,
            error_message,
            mismatches_detail
        FROM aave_ethereum.UserEModeTestResults
        ORDER BY test_timestamp DESC
        LIMIT 50
        """

        result = clickhouse_client.execute_query(query)

        test_results = []
        for row in result.result_rows:
            test = {
                "test_run_id": row[0],
                "test_timestamp": row[1],
                "total_users": row[2],
                "matching_records": row[3],
                "mismatched_records": row[4],
                "clickhouse_only_records": row[5],
                "rpc_only_records": row[6],
                "match_percentage": row[7],
                "test_duration_seconds": row[8],
                "test_status": row[9],
                "error_message": row[10],
                "mismatches_detail": row[11],
            }
            test_results.append(test)

        # Get latest test summary
        latest_test = test_results[0] if test_results else None

        context = {
            "test_results": test_results,
            "latest_test": latest_test,
        }

        return render(request, "dashboard/user_emode_tests.html", context)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


def user_collateral_tests(request):
    """User collateral test detail page with history"""
    try:
        # Get the latest test results
        query = """
        SELECT
            test_run_id,
            test_timestamp,
            total_user_assets,
            matching_records,
            mismatched_records,
            clickhouse_only_records,
            rpc_only_records,
            match_percentage,
            test_duration_seconds,
            test_status,
            error_message,
            mismatches_detail
        FROM aave_ethereum.UserCollateralTestResults
        ORDER BY test_timestamp DESC
        LIMIT 50
        """

        result = clickhouse_client.execute_query(query)

        test_results = []
        for row in result.result_rows:
            test = {
                "test_run_id": row[0],
                "test_timestamp": row[1],
                "total_user_assets": row[2],
                "matching_records": row[3],
                "mismatched_records": row[4],
                "clickhouse_only_records": row[5],
                "rpc_only_records": row[6],
                "match_percentage": row[7],
                "test_duration_seconds": row[8],
                "test_status": row[9],
                "error_message": row[10],
                "mismatches_detail": row[11],
            }
            test_results.append(test)

        # Get latest test summary
        latest_test = test_results[0] if test_results else None

        context = {
            "test_results": test_results,
            "latest_test": latest_test,
        }

        return render(request, "dashboard/user_collateral_tests.html", context)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
@csrf_exempt
@require_http_methods(["GET"])
def collateral_balance_tests(request):
    """Collateral balance test detail page with history"""
    try:
        # Get the latest test results
        query = """
        SELECT
            test_timestamp,
            total_user_assets,
            matching_records,
            mismatched_records,
            match_percentage,
            avg_difference_bps,
            max_difference_bps,
            test_duration_seconds,
            test_status,
            error_message,
            mismatches_detail
        FROM aave_ethereum.CollateralBalanceTestResults
        ORDER BY test_timestamp DESC
        LIMIT 50
        """

        result = clickhouse_client.execute_query(query)

        test_results = []
        for row in result.result_rows:
            test = {
                "test_timestamp": row[0],
                "total_user_assets": row[1],
                "matching_records": row[2],
                "mismatched_records": row[3],
                "match_percentage": row[4],
                "avg_difference_bps": row[5],
                "max_difference_bps": row[6],
                "test_duration_seconds": row[7],
                "test_status": row[8],
                "error_message": row[9],
                "mismatches_detail": row[10],
            }
            test_results.append(test)

        # Get latest test summary
        latest_test = test_results[0] if test_results else None

        context = {
            "test_results": test_results,
            "latest_test": latest_test,
        }

        return render(request, "dashboard/collateral_balance_tests.html", context)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
@csrf_exempt
@require_http_methods(["GET"])
def debt_balance_tests(request):
    """Debt balance test detail page with history"""
    try:
        # Get the latest test results
        query = """
        SELECT
            test_timestamp,
            total_user_assets,
            matching_records,
            mismatched_records,
            match_percentage,
            avg_difference_bps,
            max_difference_bps,
            test_duration_seconds,
            test_status,
            error_message,
            mismatches_detail
        FROM aave_ethereum.DebtBalanceTestResults
        ORDER BY test_timestamp DESC
        LIMIT 50
        """

        result = clickhouse_client.execute_query(query)

        test_results = []
        for row in result.result_rows:
            test = {
                "test_timestamp": row[0],
                "total_user_assets": row[1],
                "matching_records": row[2],
                "mismatched_records": row[3],
                "match_percentage": row[4],
                "avg_difference_bps": row[5],
                "max_difference_bps": row[6],
                "test_duration_seconds": row[7],
                "test_status": row[8],
                "error_message": row[9],
                "mismatches_detail": row[10],
            }
            test_results.append(test)

        # Get latest test summary
        latest_test = test_results[0] if test_results else None

        context = {
            "test_results": test_results,
            "latest_test": latest_test,
        }

        return render(request, "dashboard/debt_balance_tests.html", context)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
@csrf_exempt
@require_http_methods(["GET"])
def user_balances_api(request, user_address):
    """API endpoint to get user balances"""
    try:
        query = """
        SELECT
            lb.user,
            lb.asset,
            COALESCE(csd.is_enabled_as_collateral, 0) as is_enabled_as_collateral_total,
            sumMerge(lb.collateral_scaled_balance) as collateral_scaled_balance,
            max_idx.max_collateral_liquidityIndex as collateral_liquidityIndex_total,
            sumMerge(lb.variable_debt_scaled_balance) as variable_debt_scaled_balance,
            max_idx.max_variable_debt_liquidityIndex as variable_debt_liquidityIndex
        FROM aave_ethereum.LatestBalances_v2 lb
        LEFT JOIN aave_ethereum.MaxLiquidityIndex max_idx ON lb.asset = max_idx.asset
        LEFT JOIN aave_ethereum.CollateralStatusDictionary csd
            ON lb.user = csd.user AND lb.asset = csd.asset
        WHERE lb.user = %(user_address)s
        GROUP BY lb.user, lb.asset, csd.is_enabled_as_collateral, max_idx.max_collateral_liquidityIndex, max_idx.max_variable_debt_liquidityIndex
        ORDER BY lb.asset
        """

        result = clickhouse_client.execute_query(query, {"user_address": user_address})

        balances = []
        RAY = 1e27
        for row in result.result_rows:
            # Convert scaled balances to underlying using ray math
            scaled_collateral = float(row[3]) if row[3] else 0
            collateral_index = float(row[4]) if row[4] else 0
            scaled_debt = float(row[5]) if row[5] else 0
            debt_index = float(row[6]) if row[6] else 0

            # underlying = floor(scaled * current_index / RAY)
            collateral_balance = (
                int(scaled_collateral * collateral_index / RAY)
                if collateral_index > 0
                else 0
            )
            debt_balance = int(scaled_debt * debt_index / RAY) if debt_index > 0 else 0

            balance = {
                "user": row[0],
                "asset": row[1],
                "is_enabled_as_collateral": int(row[2]) if row[2] else 0,
                "collateral_balance": collateral_balance,
                "collateral_liquidityIndex": int(collateral_index),
                "variable_debt_balance": debt_balance,
                "variable_debt_liquidityIndex": int(debt_index),
            }
            balances.append(balance)

        return JsonResponse({"balances": balances})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
@csrf_exempt
@require_http_methods(["GET"])
def user_events_api(request, user_address, asset):
    """API endpoint to get user events for a specific asset"""
    try:
        # Get BalanceTransfer events
        balance_transfer_query = """
        SELECT
            'BalanceTransfer' as event_type,
            blockNumber,
            blockTimestamp,
            transactionHash,
            _from,
            _to,
            value,
            index,
            type
        FROM aave_ethereum.BalanceTransfer
        WHERE ((_from = %(user_address)s) OR (_to = %(user_address)s)) AND asset = %(asset)s
        ORDER BY blockTimestamp DESC
        LIMIT 100
        """

        # Get Mint events
        mint_query = """
        SELECT
            'Mint' as event_type,
            blockNumber,
            blockTimestamp,
            transactionHash,
            onBehalfOf,
            '' as _to,
            value,
            index,
            type
        FROM aave_ethereum.Mint
        WHERE onBehalfOf = %(user_address)s AND asset = %(asset)s
        ORDER BY blockTimestamp DESC
        LIMIT 100
        """

        # Get Burn events
        burn_query = """
        SELECT
            'Burn' as event_type,
            blockNumber,
            blockTimestamp,
            transactionHash,
            from,
            '' as _to,
            value,
            index,
            type
        FROM aave_ethereum.Burn
        WHERE from = %(user_address)s AND asset = %(asset)s
        ORDER BY blockTimestamp DESC
        LIMIT 100
        """

        params = {"user_address": user_address, "asset": asset}

        balance_transfers = clickhouse_client.execute_query(
            balance_transfer_query, params
        )
        mints = clickhouse_client.execute_query(mint_query, params)
        burns = clickhouse_client.execute_query(burn_query, params)

        events = []

        # Process BalanceTransfer events
        for row in balance_transfers.result_rows:
            event = {
                "event_type": row[0],
                "block_number": row[1],
                "block_timestamp": row[2].isoformat() if row[2] else None,
                "transaction_hash": row[3],
                "from": row[4],
                "to": row[5],
                "value": float(row[6]) if row[6] else 0,
                "index": int(row[7]) if row[7] else 0,
                "type": row[8],
            }
            events.append(event)

        # Process Mint events
        for row in mints.result_rows:
            event = {
                "event_type": row[0],
                "block_number": row[1],
                "block_timestamp": row[2].isoformat() if row[2] else None,
                "transaction_hash": row[3],
                "from": "",
                "to": row[4],
                "value": float(row[6]) if row[6] else 0,
                "index": int(row[7]) if row[7] else 0,
                "type": row[8],
            }
            events.append(event)

        # Process Burn events
        for row in burns.result_rows:
            event = {
                "event_type": row[0],
                "block_number": row[1],
                "block_timestamp": row[2].isoformat() if row[2] else None,
                "transaction_hash": row[3],
                "from": row[4],
                "to": "",
                "value": float(row[6]) if row[6] else 0,
                "index": int(row[7]) if row[7] else 0,
                "type": row[8],
            }
            events.append(event)

        # Sort all events by timestamp
        events.sort(key=lambda x: x["block_timestamp"] or "", reverse=True)

        return JsonResponse({"events": events})

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


def user_events_view(request):
    """User events view page"""
    return render(request, "dashboard/user_events.html")


def user_asset_events_api(request, user_address, asset):
    """API endpoint to get user events for a specific asset, including aToken and variableDebt addresses"""
    try:
        # Get asset configuration from ClickHouse to retrieve aToken and variableDebt addresses
        asset_query = f"""
        SELECT
            asset,
            aToken,
            variableDebtToken
        FROM aave_ethereum.view_LatestAssetConfiguration
        WHERE asset = '{asset}'
        """

        asset_result = clickhouse_client.execute_query(asset_query)

        if not asset_result.result_rows:
            return JsonResponse(
                {"error": f"Asset {asset} not found in ClickHouse"}, status=404
            )

        asset_row = asset_result.result_rows[0]
        atoken_address = asset_row[1]
        variable_debt_address = asset_row[2]

        # Build queries for mint, burn, and transfer events for both aToken and variableDebt
        # We'll query using the token addresses (aToken and variableDebt)

        events = []

        # Get all relevant token addresses
        token_addresses = []
        if atoken_address:
            token_addresses.append(atoken_address)
        if variable_debt_address:
            token_addresses.append(variable_debt_address)

        if not token_addresses:
            return JsonResponse(
                {
                    "error": "No aToken or variableDebt token addresses found for this asset",
                    "atoken_address": atoken_address,
                    "variable_debt_address": variable_debt_address,
                    "events": [],
                }
            )

        # Build WHERE clause for token addresses
        # Create individual params for each token address
        token_conditions = []
        params = {"user_address": user_address}

        for idx, token_addr in enumerate(token_addresses):
            param_name = f"token_addr_{idx}"
            params[param_name] = token_addr
            token_conditions.append(f"address = %({param_name})s")

        address_filter = " OR ".join(token_conditions) if token_conditions else "1=0"

        # Query Mint events
        mint_query = f"""
        SELECT
            'Mint' as event_type,
            blockNumber,
            blockTimestamp,
            transactionHash,
            address,
            onBehalfOf,
            value,
            balanceIncrease,
            index,
            type
        FROM aave_ethereum.Mint
        WHERE onBehalfOf = %(user_address)s
            AND ({address_filter})
        ORDER BY blockTimestamp DESC
        LIMIT 200
        """

        # Query Burn events
        burn_query = f"""
        SELECT
            'Burn' as event_type,
            blockNumber,
            blockTimestamp,
            transactionHash,
            address,
            `from`,
            target,
            value,
            balanceIncrease,
            index,
            type
        FROM aave_ethereum.Burn
        WHERE `from` = %(user_address)s
            AND ({address_filter})
        ORDER BY blockTimestamp DESC
        LIMIT 200
        """

        # Query Transfer events (BalanceTransfer)
        transfer_query = f"""
        SELECT
            'Transfer' as event_type,
            blockNumber,
            blockTimestamp,
            transactionHash,
            address,
            _from,
            _to,
            value,
            `index`,
            type
        FROM aave_ethereum.BalanceTransfer
        WHERE ((_from = %(user_address)s) OR (_to = %(user_address)s))
            AND ({address_filter})
        ORDER BY blockTimestamp DESC
        LIMIT 200
        """

        # Execute queries
        mint_results = clickhouse_client.execute_query(mint_query, params)
        burn_results = clickhouse_client.execute_query(burn_query, params)
        transfer_results = clickhouse_client.execute_query(transfer_query, params)

        # Helper function to safely format timestamp
        def format_timestamp(ts):
            if not ts:
                return None
            try:
                from datetime import datetime

                # If it's a numeric timestamp, convert to datetime
                if isinstance(ts, (int, float)):
                    # Try interpreting as seconds first
                    dt = datetime.fromtimestamp(ts)
                    return dt.isoformat()
                # If it has a timestamp method (datetime object)
                elif hasattr(ts, "timestamp"):
                    # Convert to datetime first to avoid year out of range
                    return datetime.fromtimestamp(int(ts.timestamp())).isoformat()
                # If it has isoformat method
                elif hasattr(ts, "isoformat"):
                    return ts.isoformat()
                # Otherwise convert to string
                return str(ts)
            except Exception as e:
                import logging

                logger = logging.getLogger(__name__)
                logger.error(
                    f"Error formatting timestamp {ts} (type: {type(ts)}): {str(e)}"
                )
                # Return as string for debugging
                return str(ts)

        # Process Mint events
        for row in mint_results.result_rows:
            event = {
                "event_type": row[0],
                "block_number": row[1],
                "block_timestamp": format_timestamp(row[2]),
                "transaction_hash": row[3],
                "token_address": row[4],
                "on_behalf_of": row[5],
                "value": float(row[6]) if row[6] else 0,
                "balance_increase": float(row[7]) if row[7] else 0,
                "liquidity_index": float(row[8]) if row[8] else 0,
                "type": row[9],
            }
            events.append(event)

        # Process Burn events
        for row in burn_results.result_rows:
            event = {
                "event_type": row[0],
                "block_number": row[1],
                "block_timestamp": format_timestamp(row[2]),
                "transaction_hash": row[3],
                "token_address": row[4],
                "from_address": row[5],
                "target": row[6],
                "value": float(row[7]) if row[7] else 0,
                "balance_increase": float(row[8]) if row[8] else 0,
                "liquidity_index": float(row[9]) if row[9] else 0,
                "type": row[10],
            }
            events.append(event)

        # Process Transfer events
        for row in transfer_results.result_rows:
            event = {
                "event_type": row[0],
                "block_number": row[1],
                "block_timestamp": format_timestamp(row[2]),
                "transaction_hash": row[3],
                "token_address": row[4],
                "from_address": row[5],
                "to_address": row[6],
                "value": float(row[7]) if row[7] else 0,
                "liquidity_index": float(row[8]) if row[8] else 0,
                "type": row[9],
            }
            events.append(event)

        # Sort all events by timestamp
        events.sort(key=lambda x: x["block_timestamp"] or 0, reverse=True)

        return JsonResponse(
            {
                "atoken_address": atoken_address,
                "variable_debt_address": variable_debt_address,
                "events": events,
            }
        )

    except Exception as e:
        import traceback

        return JsonResponse(
            {"error": str(e), "traceback": traceback.format_exc()}, status=500
        )

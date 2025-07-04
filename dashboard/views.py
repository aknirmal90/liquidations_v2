from datetime import datetime

from bokeh.embed import components
from bokeh.models import ColumnDataSource, DatetimeTickFormatter, HoverTool
from bokeh.plotting import figure
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from utils.clickhouse.client import clickhouse_client
from utils.constants import NETWORK_NAME


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
            stableDebtToken,
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
            name
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
                        {"timestamp": row[0], "max_cap": max_cap_value, "name": row[2]}
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
                "stableDebtToken": asset_config[2],
                "variableDebtToken": asset_config[3],
                "interest_rate_strategy": asset_config[4],
                "name": asset_config[5] or "Unknown",
                "symbol": asset_config[6] or "Unknown",
                "price_event_name": event_price_data.get("name")
                if event_price_data
                else "Unknown",
                "decimals": asset_config[7] or 18,
                "decimals_places": asset_config[8] or 18,
                "collateral_ltv": round(asset_config[9] / 100.0, 2) or 0,
                "collateral_liquidation_threshold": round(asset_config[10] / 100.0, 2)
                or 0,
                "collateral_liquidation_bonus": max(
                    round((asset_config[11] / 100.0) - 100.00, 2) or 0, 0
                ),
                "emode_category_id": asset_config[12],
                "emode_ltv": round(asset_config[13] / 100.0, 2) or 0,
                "emode_liquidation_threshold": round(asset_config[14] / 100.0, 2) or 0,
                "emode_liquidation_bonus": max(
                    round((asset_config[15] / 100.0) - 100.00, 2) or 0, 0
                ),
                "current_price": current_price,
                "asset_source": asset_source,
                "avg_block_time": avg_block_time,
            },
            "addresses": {
                "asset": get_simple_explorer_url(asset_config[0]),
                "aToken": get_simple_explorer_url(asset_config[1]),
                "stableDebtToken": get_simple_explorer_url(asset_config[2]),
                "variableDebtToken": get_simple_explorer_url(asset_config[3]),
                "interest_rate_strategy": get_simple_explorer_url(asset_config[4]),
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
        # Get initial box plot data for default time window (1 month)
        time_window = request.GET.get("time_window", "1_month")

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
                    PARTITION BY asset
                    ORDER BY blockTimestamp DESC
                ) as rn
            FROM aave_ethereum.PriceVerificationRecords
            WHERE blockTimestamp >= now() - INTERVAL 30 DAY
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY asset, asset_source, name) as row_number,
            asset,
            asset_source,
            name,
            SUM(CASE WHEN type = 'historical_event' THEN pct_error ELSE NULL END) as historical_event_error,
            SUM(CASE WHEN type = 'historical_transaction' THEN pct_error ELSE NULL END) as historical_transaction_error,
            SUM(CASE WHEN type = 'predicted_transaction' THEN pct_error ELSE NULL END) as predicted_transaction_error
        FROM LatestRecords
        WHERE rn = 1
        GROUP BY asset, asset_source, name
        ORDER BY asset, asset_source, name
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
                    "historical_event_error": round(row[4], 6)
                    if row[4] is not None
                    else None,
                    "historical_transaction_error": round(row[5], 6)
                    if row[5] is not None
                    else None,
                    "predicted_transaction_error": round(row[6], 6)
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


def get_box_plot_data(time_window="1_month"):
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

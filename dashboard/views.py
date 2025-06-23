from datetime import datetime

from bokeh.embed import components
from bokeh.models import DatetimeTickFormatter, HoverTool
from bokeh.plotting import figure
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from utils.admin import get_explorer_address_url
from utils.clickhouse.client import clickhouse_client


@login_required
def asset_list(request):
    """View to list all assets in the database"""
    try:
        # Query all assets from the LatestAssetConfiguration view
        query = """
        SELECT DISTINCT
            asset,
            name,
            symbol,
            decimals,
            collateralLTV,
            collateralLiquidationThreshold,
            collateralLiquidationBonus
        FROM aave_ethereum.view_LatestAssetConfiguration
        ORDER BY collateralLiquidationBonus DESC
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

        # Get current price from LatestRawPriceEvent
        price_query = f"""
        SELECT
            asset,
            price,
            blockTimestamp
        FROM aave_ethereum.LatestRawPriceEvent
        WHERE asset = '{asset_address}'
        """

        price_result = clickhouse_client.execute_query(price_query)
        current_price = None
        asset_source = None
        if price_result.result_rows:
            try:
                current_price = int(price_result.result_rows[0][1])
            except (ValueError, TypeError, IndexError):
                current_price = None

        # Get AssetSourceUpdated events
        source_events_query = f"""
        SELECT
            asset,
            source as asset_source,
            blockTimestamp,
            blockNumber
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
                    "block_timestamp": row[2],
                    "block_number": row[3],
                }
            )

        # Get historical price data for timeseries
        historical_prices_query = f"""
        SELECT
            price,
            blockTimestamp
        FROM aave_ethereum.RawPriceEvent
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

        # Calculate average time between consecutive blocks
        avg_block_time = calculate_average_block_time(historical_prices)

        # Create price visualizations
        price_plots = create_price_plots(historical_prices, asset_config[6])  # symbol

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
                "asset": get_explorer_address_url(asset_config[0]),
                "aToken": get_explorer_address_url(asset_config[1]),
                "stableDebtToken": get_explorer_address_url(asset_config[2]),
                "variableDebtToken": get_explorer_address_url(asset_config[3]),
                "interest_rate_strategy": get_explorer_address_url(asset_config[4]),
            },
            "source_events": source_events,
            "price_plots": price_plots,
            "plots": plots,
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

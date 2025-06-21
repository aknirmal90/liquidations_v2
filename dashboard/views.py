from bokeh.embed import components
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
            },
            "addresses": {
                "asset": get_explorer_address_url(asset_config[0]),
                "aToken": get_explorer_address_url(asset_config[1]),
                "stableDebtToken": get_explorer_address_url(asset_config[2]),
                "variableDebtToken": get_explorer_address_url(asset_config[3]),
                "interest_rate_strategy": get_explorer_address_url(asset_config[4]),
            },
        }

        return render(request, "dashboard/asset_detail.html", context)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


def create_asset_plots(asset_config, historical_data):
    """Create Bokeh plots for asset visualization"""
    plots = {}

    # Create configuration comparison chart
    config_fig = figure(
        title=f"{asset_config[2]} Configuration Parameters",
        x_range=["LTV", "Liquidation Threshold", "Liquidation Bonus"],
        height=400,
        width=600,
        tools="pan,wheel_zoom,box_zoom,reset,save",
    )

    # Data for configuration chart
    categories = ["LTV", "Liquidation Threshold", "Liquidation Bonus"]
    collateral_values = [
        asset_config[5] or 0,  # collateralLTV
        asset_config[6] or 0,  # collateralLiquidationThreshold
        asset_config[7] or 0,  # collateralLiquidationBonus
    ]
    emode_values = [
        asset_config[9] or 0,  # eModeLTV
        asset_config[10] or 0,  # eModeLiquidationThreshold
        asset_config[11] or 0,  # eModeLiquidationBonus
    ]

    # Create bars for collateral and eMode configurations
    config_fig.vbar(
        x=categories,
        top=collateral_values,
        width=0.3,
        color="#1f77b4",
        legend_label="Collateral",
        alpha=0.7,
    )
    config_fig.vbar(
        x=categories,
        top=emode_values,
        width=0.3,
        color="#ff7f0e",
        legend_label="E-Mode",
        alpha=0.7,
    )

    config_fig.yaxis.axis_label = "Percentage (%)"
    config_fig.legend.location = "top_right"
    config_fig.legend.click_policy = "hide"

    plots["config_chart"] = components(config_fig)

    # Create historical events chart if data exists
    if historical_data:
        dates = [str(row[0]) for row in historical_data]
        counts = [row[1] for row in historical_data]

        # Reverse to show chronological order
        dates.reverse()
        counts.reverse()

        history_fig = figure(
            title=f"{asset_config[2]} Historical Events",
            x_axis_type="datetime",
            height=400,
            width=600,
            tools="pan,wheel_zoom,box_zoom,reset,save",
        )

        history_fig.line(dates, counts, line_width=2, color="#2ca02c")
        history_fig.circle(dates, counts, size=8, color="#2ca02c", alpha=0.7)

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

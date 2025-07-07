from django.urls import path

from . import views

app_name = "dashboard"

urlpatterns = [
    path("", views.prices_summary, name="prices_summary"),
    path("assets/", views.asset_list, name="asset_list"),
    path("liquidations/", views.liquidations, name="liquidations"),
    path("asset/<str:asset_address>/", views.asset_detail, name="asset_detail"),
    path("api/asset/<str:asset_address>/", views.asset_data_api, name="asset_data_api"),
    path(
        "api/price-box-plot-data/",
        views.price_box_plot_data,
        name="price_box_plot_data",
    ),
    path(
        "api/price-mismatch-counts-data/",
        views.price_mismatch_counts_data,
        name="price_mismatch_counts_data",
    ),
    path(
        "api/price-zero-error-stats-data/",
        views.price_zero_error_stats_data,
        name="price_zero_error_stats_data",
    ),
    path(
        "api/transaction-coverage-metrics/",
        views.transaction_coverage_metrics,
        name="transaction_coverage_metrics",
    ),
    path(
        "api/transaction-timestamp-differences/",
        views.transaction_timestamp_differences,
        name="transaction_timestamp_differences",
    ),
    path(
        "api/liquidations-metrics/",
        views.liquidations_metrics,
        name="liquidations_metrics",
    ),
    path(
        "api/liquidations-top-liquidators/",
        views.liquidations_top_liquidators,
        name="liquidations_top_liquidators",
    ),
    path(
        "api/liquidations-timeseries/",
        views.liquidations_timeseries,
        name="liquidations_timeseries",
    ),
    path(
        "api/liquidations-recent/",
        views.liquidations_recent,
        name="liquidations_recent",
    ),
    path(
        "liquidations/detail/<str:transaction_hash>/",
        views.liquidation_detail,
        name="liquidation_detail",
    ),
    path(
        "liquidations/liquidator/<str:liquidator_address>/",
        views.liquidator_detail,
        name="liquidator_detail",
    ),
]

from django.urls import path

from . import views

app_name = "dashboard"

urlpatterns = [
    path("", views.asset_list, name="asset_list"),
    path("prices/", views.prices_summary, name="prices_summary"),
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
]

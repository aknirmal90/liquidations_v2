from django.urls import path

from . import views

app_name = "dashboard"

urlpatterns = [
    path("", views.asset_list, name="asset_list"),
    path("asset/<str:asset_address>/", views.asset_detail, name="asset_detail"),
    path("api/asset/<str:asset_address>/", views.asset_data_api, name="asset_data_api"),
]

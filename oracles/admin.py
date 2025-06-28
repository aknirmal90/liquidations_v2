from django.contrib import admin
from django.utils.html import format_html

from oracles.models import PriceEvent
from utils.admin import (
    EnableDisableAdminMixin,
    format_pretty_json,
    get_explorer_address_url,
)


@admin.register(PriceEvent)
class PriceEventAdmin(EnableDisableAdminMixin, admin.ModelAdmin):
    list_display = (
        "name",
        "asset_source_name",
        "get_asset_display",
        "get_asset_source_display",
        "is_enabled",
        "last_synced_block",
        "blocks_to_sync",
        "logs_count",
        "get_contracts_display",
        "updated_at",
    )

    list_filter = ("is_enabled", "updated_at", "asset_source_name", "name")

    search_fields = ("name", "signature", "topic_0", "asset", "asset_source")

    fieldsets = (
        ("Basic Information", {"fields": ("name", "asset_source_name", "is_enabled")}),
        (
            "Asset Information",
            {"fields": ("get_asset_display", "get_asset_source_display")},
        ),
        (
            "Sync Status",
            {"fields": ("last_synced_block", "blocks_to_sync", "logs_count")},
        ),
        (
            "Event Details",
            {
                "fields": (
                    "signature",
                    "topic_0",
                    "abi_display",
                    "contract_addresses",
                    "method_ids",
                )
            },
        ),
        ("Timestamps", {"fields": ("created_at", "updated_at")}),
    )

    readonly_fields = (
        "created_at",
        "updated_at",
        "blocks_to_sync",
        "name",
        "signature",
        "abi",
        "topic_0",
        "abi_display",
        "contract_addresses",
        "logs_count",
        "is_enabled",
        "get_contracts_display",
        "get_asset_display",
        "get_asset_source_display",
        "asset_source_name",
        "method_ids",
    )

    def abi_display(self, obj):
        return format_pretty_json(obj.abi)

    abi_display.short_description = "ABI"

    def get_contracts_display(self, obj):
        contract_links = []
        for contract in obj.contract_addresses:
            contract_links.append(get_explorer_address_url(contract))
        return format_html("<br>".join(contract_links))

    get_contracts_display.short_description = "Contract Addresses"

    def get_asset_display(self, obj):
        return get_explorer_address_url(obj.asset)

    get_asset_display.short_description = "Asset"

    def get_asset_source_display(self, obj):
        return get_explorer_address_url(obj.asset_source)

    get_asset_source_display.short_description = "Asset Source"

    def reset_sync_status(self, request, queryset):
        queryset.update(last_synced_block=0, logs_count=0)

    reset_sync_status.short_description = "Reset Sync Status"

    actions = (
        "reset_sync_status",
        "enable",
        "disable",
    )

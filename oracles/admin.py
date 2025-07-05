from django.contrib import admin, messages
from django.utils.html import format_html

from oracles.models import PriceEvent
from utils.admin import (
    EnableDisableAdminMixin,
    format_pretty_json,
    get_explorer_address_url,
)
from utils.clickhouse.client import clickhouse_client


@admin.register(PriceEvent)
class PriceEventAdmin(EnableDisableAdminMixin, admin.ModelAdmin):
    list_display = (
        "name",
        "asset_source_name",
        "get_asset_display",
        "get_asset_source_display",
        "is_enabled",
        "last_synced_block",
        "last_inserted_block",
        "blocks_to_sync",
        "logs_count",
        "get_contracts_display",
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
            "Transmitters",
            {"fields": ("get_transmitters_display",)},
        ),
        (
            "Sync Status",
            {
                "fields": (
                    "last_synced_block",
                    "blocks_to_sync",
                    "last_inserted_block",
                    "logs_count",
                )
            },
        ),
        (
            "Event Details",
            {
                "fields": (
                    "signature",
                    "topic_0",
                    "abi_display",
                    "contract_addresses",
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
        "transmitters",
        "get_transmitters_display",
        "last_inserted_block",
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

    def get_transmitters_display(self, obj):
        if not obj.transmitters:
            return

        transmitter_links = []
        for transmitter in obj.transmitters:
            transmitter_links.append(get_explorer_address_url(transmitter))
        return format_html("<br>".join(transmitter_links))

    get_transmitters_display.short_description = "Transmitters"

    def get_asset_display(self, obj):
        return get_explorer_address_url(obj.asset)

    get_asset_display.short_description = "Asset"

    def get_asset_source_display(self, obj):
        return get_explorer_address_url(obj.asset_source)

    get_asset_source_display.short_description = "Asset Source"

    def reset_sync_status(self, request, queryset):
        """Reset sync status and delete oracle records for selected asset-source pairs"""
        try:
            # Extract asset-source pairs from selected PriceEvents
            asset_source_pairs = [(pe.asset, pe.asset_source) for pe in queryset]

            # Reset the sync status for selected PriceEvents
            updated_count = queryset.update(last_synced_block=0, logs_count=0)

            # Delete oracle records only for the selected asset-source combinations
            deletion_results = clickhouse_client.delete_oracle_records_by_asset_source(
                asset_source_pairs
            )

            # Count successful deletions
            successful_tables = [
                table
                for table, result in deletion_results.items()
                if result == "success"
            ]
            failed_tables = [
                table
                for table, result in deletion_results.items()
                if result.startswith("error")
            ]

            success_message = f"Reset sync status for {updated_count} PriceEvent(s) and deleted oracle records from {len(successful_tables)} tables for {len(asset_source_pairs)} asset-source pairs."

            if failed_tables:
                success_message += (
                    f" Warning: {len(failed_tables)} tables had deletion errors."
                )

            self.message_user(request, success_message, messages.SUCCESS)

            if failed_tables:
                self.message_user(
                    request,
                    f"Tables with deletion errors: {', '.join(failed_tables[:5])}{'...' if len(failed_tables) > 5 else ''}",
                    messages.WARNING,
                )

        except Exception as e:
            self.message_user(request, f"Error during reset: {str(e)}", messages.ERROR)

    reset_sync_status.short_description = (
        "Reset Sync Status & Delete Oracle Records (Selected Assets)"
    )

    actions = (
        "reset_sync_status",
        "enable",
        "disable",
    )

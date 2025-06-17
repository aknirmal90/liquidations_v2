from django.contrib import admin
from django.utils.html import format_html

from blockchains.models import Event
from utils.admin import (
    EnableDisableAdminMixin,
    format_pretty_json,
    get_explorer_address_url,
)


@admin.register(Event)
class EventAdmin(EnableDisableAdminMixin, admin.ModelAdmin):
    list_display = (
        "name",
        "is_enabled",
        "last_synced_block",
        "blocks_to_sync",
        "get_contracts_display",
        "updated_at",
    )

    list_filter = ("is_enabled", "updated_at")

    search_fields = ("name", "signature", "topic_0", "model_class")

    fieldsets = (
        ("Basic Information", {"fields": ("name", "is_enabled")}),
        ("Sync Status", {"fields": ("last_synced_block", "blocks_to_sync")}),
        (
            "Event Details",
            {"fields": ("signature", "topic_0", "abi_display", "contract_addresses")},
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
        "is_enabled",
        "get_contracts_display",
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

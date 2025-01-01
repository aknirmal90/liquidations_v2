from django.contrib import admin
from django.db import models
from django.forms import Textarea

from blockchains.models import Event, Network, Protocol
from utils.admin import EnableDisableAdminMixin, format_pretty_json


@admin.register(Protocol)
class ProtocolAdmin(EnableDisableAdminMixin, admin.ModelAdmin):
    list_display = (
        'name',
        'is_enabled'
    )

    list_filter = (
        'is_enabled',
    )


@admin.register(Network)
class NetworkAdmin(admin.ModelAdmin):
    list_display = (
        'name',
        'chain_id',
        'rpc',
        'latest_block'
    )

    formfield_overrides = {
        models.URLField: {'widget': Textarea(attrs={'rows': 5, 'cols': '100'})},
        models.CharField: {'widget': Textarea(attrs={'rows': 5, 'cols': '100'})},
    }

    readonly_fields = (
        'name',
        'latest_block',
        'chain_id',
    )


@admin.register(Event)
class EventAdmin(EnableDisableAdminMixin, admin.ModelAdmin):
    list_display = (
        'name',
        'protocol',
        'network',
        'is_enabled',
        'last_synced_block',
        'blocks_to_sync',
        'contract_addresses',
        'updated_at'
    )

    list_filter = (
        'is_enabled',
        'protocol',
        'network',
        'updated_at'
    )

    search_fields = (
        'name',
        'signature',
        'topic_0',
        'model_class'
    )

    fieldsets = (
        ('Basic Information', {
            'fields': (
                'name',
                'protocol',
                'network',
                'is_enabled'
            )
        }),
        ('Sync Status', {
            'fields': (
                'last_synced_block',
                'blocks_to_sync'
            )
        }),
        ('Event Details', {
            'fields': (
                'signature',
                'topic_0',
                'model_class',
                'abi_display',
                'contract_addresses'
            )
        }),
        ('Timestamps', {
            'fields': (
                'created_at',
                'updated_at'
            )
        })
    )

    readonly_fields = (
        'created_at',
        'updated_at',
        'blocks_to_sync',
        'name',
        'protocol',
        'network',
        'signature',
        'abi',
        'topic_0',
        'model_class',
        'abi_display',
        'contract_addresses'
    )

    def abi_display(self, obj):
        return format_pretty_json(obj.abi)

    abi_display.short_description = 'ABI'

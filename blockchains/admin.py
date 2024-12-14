from django.contrib import admin
from django.db import models
from django.forms import Textarea

from blockchains.models import Network, Protocol
from utils.admin import EnableDisableAdminMixin


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
        'rpc',
        'latest_block'
    )

    formfield_overrides = {
        models.URLField: {'widget': Textarea(attrs={'rows': 5, 'cols': '100'})},
    }

    readonly_fields = (
        'name',
        'latest_block',
        'rpc_adapter_path'
    )

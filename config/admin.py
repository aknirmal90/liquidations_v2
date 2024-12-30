from django.contrib import admin
from django.db import models
from django.forms import Textarea

from .models import Configuration


@admin.register(Configuration)
class ConfigurationAdmin(admin.ModelAdmin):
    list_display = ('key', 'value', 'type')
    list_filter = ('type',)
    search_fields = ('key', 'value')
    ordering = ('key',)

    fieldsets = (
        (None, {
            'fields': ('key', 'value', 'type')
        }),
    )

    formfield_overrides = {
        models.CharField: {'widget': Textarea(attrs={'rows': 4, 'cols': 85})},
    }

from django.apps import AppConfig
from django.db.models.signals import pre_save


class AaveConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "aave"

    def ready(self):
        from aave.models import Asset
        from aave.signals import update_asset_cache
        pre_save.connect(update_asset_cache, sender=Asset)

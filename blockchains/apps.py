from django.apps import AppConfig
from django.db.models.signals import pre_save


class BlockchainsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "blockchains"

    def ready(self):
        from blockchains.models import Network, Protocol
        from blockchains.signals import update_network_cache, update_protocol_cache

        pre_save.connect(update_protocol_cache, sender=Protocol)
        pre_save.connect(update_network_cache, sender=Network)

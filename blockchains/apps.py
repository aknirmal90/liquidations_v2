from django.apps import AppConfig


class BlockchainsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "blockchains"

    def ready(self):
        from blockchains import signals  # noqa: F401

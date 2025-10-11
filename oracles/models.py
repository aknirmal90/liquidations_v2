from django.db import models

from utils.models import BaseEvent


class PriceEvent(BaseEvent):
    asset = models.CharField(max_length=256, null=False)
    asset_source = models.CharField(max_length=256, null=False)
    asset_source_name = models.CharField(max_length=256, null=False)
    last_inserted_block = models.IntegerField(default=0)
    is_active = models.BooleanField(default=False)

    transmitters = models.JSONField(null=True, blank=True)
    authorized_senders = models.JSONField(null=True, blank=True)

    class Meta:
        unique_together = ("asset", "asset_source", "topic_0")

from django.db import models

from oracles.contracts.service import get_contract_interface
from utils.models import BaseEvent


class PriceEvent(BaseEvent):
    asset = models.CharField(max_length=256, null=False)
    asset_source = models.CharField(max_length=256, null=False)
    asset_source_name = models.CharField(max_length=256, null=False)
    method_ids = models.JSONField(null=True, blank=True)

    class Meta:
        unique_together = ("asset", "asset_source", "topic_0")

    @property
    def contract_interface(self):
        return get_contract_interface(self.asset, self.asset_source)

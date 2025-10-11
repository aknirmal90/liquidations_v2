from django.db import models

from utils.models import BaseEvent
from utils.rpc import rpc_adapter


class PriceEvent(BaseEvent):
    asset = models.CharField(max_length=256, null=False)
    asset_source = models.CharField(max_length=256, null=False)
    asset_source_name = models.CharField(max_length=256, null=False)
    last_inserted_block = models.IntegerField(default=0)
    is_active = models.BooleanField(default=False)

    transmitters = models.JSONField(null=True, blank=True)

    class Meta:
        unique_together = ("asset", "asset_source", "topic_0")

    def get_transmitters(self):
        return rpc_adapter.extract_raw_event_data(
            topics=[
                "0x78af32efdcad432315431e9b03d27e6cd98fb79c405fdc5af7c1714d9c0f75b3"
            ],
            # EX: https://etherscan.io/tx/0xd5127d96ca9519c89cda89a57d69437d654da21f773f992f469d4e7128977da1#eventlog
            contract_address=self.contract_addresses,
        )

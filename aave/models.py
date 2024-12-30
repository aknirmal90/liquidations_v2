import logging
import math
from decimal import Decimal

from django.core.cache import cache
from django.core.serializers import deserialize, serialize
from django.db import models

from blockchains.models import Network, Protocol
from utils.tokens import EvmTokenRetriever

logger = logging.getLogger(__name__)


class Asset(models.Model):

    asset = models.CharField(max_length=255, null=False, blank=False)
    protocol = models.ForeignKey(Protocol, on_delete=models.PROTECT, null=False)
    network = models.ForeignKey(Network, on_delete=models.PROTECT, null=False)

    atoken_address = models.CharField(max_length=255, null=True, blank=True)
    stable_debt_token_address = models.CharField(max_length=255, null=True, blank=True)
    variable_debt_token_address = models.CharField(max_length=255, null=True, blank=True)
    interest_rate_strategy_address = models.CharField(max_length=255, null=True, blank=True)

    symbol = models.CharField(max_length=255)
    num_decimals = models.DecimalField(
        default=Decimal("1"), max_digits=3, decimal_places=0
    )
    decimals = models.DecimalField(max_digits=72, decimal_places=0, default=Decimal("0"))
    is_enabled = models.BooleanField(default=False)

    liquidation_threshold = models.DecimalField(
        max_digits=12, decimal_places=6, null=True, blank=True
    )
    liquidation_bonus = models.DecimalField(
        max_digits=12, decimal_places=6, null=True, blank=True
    )

    emode_liquidation_threshold = models.DecimalField(
        max_digits=12, decimal_places=6, null=True, blank=True
    )
    emode_liquidation_bonus = models.DecimalField(
        max_digits=12, decimal_places=6, null=True, blank=True
    )

    pricesource = models.CharField(max_length=255, null=True, blank=True)

    contractA = models.CharField(max_length=255, null=True, blank=True)
    contractB = models.CharField(max_length=255, null=True, blank=True)

    class PriceType:
        CONSTANT = 'constant'
        AGGREGATOR = 'aggregator'
        MAX_CAPPED = 'max_capped'
        RATIO = 'ratio'

    PRICE_TYPE_CHOICES = [
        (PriceType.CONSTANT, 'Constant'),
        (PriceType.AGGREGATOR, 'Aggregator'),
        (PriceType.MAX_CAPPED, 'Max Capped'),
        (PriceType.RATIO, 'Ratio')
    ]
    price_type = models.CharField(max_length=255, choices=PRICE_TYPE_CHOICES, null=True, blank=True)

    priceA = models.DecimalField(
        max_digits=72, decimal_places=0, null=True, blank=True
    )

    priceB = models.DecimalField(
        max_digits=72, decimal_places=0, null=True, blank=True
    )

    numerator = models.DecimalField(
        max_digits=36, decimal_places=0, null=True, blank=True
    )

    denominator = models.DecimalField(
        max_digits=36, decimal_places=0, null=True, blank=True
    )

    price = models.DecimalField(
        max_digits=72, decimal_places=36, null=True, blank=True
    )
    price_in_usdt = models.DecimalField(
        max_digits=72, decimal_places=36, null=True, blank=True
    )

    updated_at_block_heightA = models.PositiveIntegerField(default=0)
    updated_at_block_heightB = models.PositiveIntegerField(default=0)

    updated_at_onchainA = models.PositiveIntegerField(default=0)
    updated_at_onchainB = models.PositiveIntegerField(default=0)

    updated_at_db_A = models.PositiveIntegerField(default=0)
    updated_at_db_B = models.PositiveIntegerField(default=0)

    emode_category = models.PositiveSmallIntegerField(default=0)
    borrowable_in_isolation_mode = models.BooleanField(default=False)
    is_reserve_paused = models.BooleanField(default=False)

    reserve_factor = models.DecimalField(
        max_digits=12, decimal_places=6, null=True, blank=True
    )
    reserve_is_flash_loan_enabled = models.BooleanField(default=True)
    reserve_is_borrow_enabled = models.BooleanField(default=True)
    reserve_is_frozen = models.BooleanField(default=False)

    emode_is_collateral = models.BooleanField(default=False)
    emode_is_borrowable = models.BooleanField(default=False)

    def __str__(self) -> str:
        return f"{self.symbol} on {self.protocol}"

    class Meta:
        unique_together = ('network', 'protocol', 'asset')
        app_label = 'aave'

    @classmethod
    def get_cache_key(cls, protocol_name: str, network_name: str, token_address: str):
        return f"asset-aave-{protocol_name}-{network_name}-{token_address}"

    @classmethod
    def get(cls, protocol_name: str, network_name: str, token_address: str):
        if protocol_name is None or network_name is None or token_address is None:
            return

        key = cls.get_cache_key(protocol_name=protocol_name, network_name=network_name, token_address=token_address)
        serialized_value = cache.get(key)

        if serialized_value:
            return next(deserialize("json", serialized_value)).object
        else:
            try:
                token = cls.objects.get(
                    asset__iexact=token_address,
                    network__name=network_name,
                    protocol__name=protocol_name
                )
                deserialized_value = serialize("json", [token])
                cache.set(key, deserialized_value)
                return token
            except cls.DoesNotExist:
                token_retriever = EvmTokenRetriever(network_name=network_name, token_address=token_address)
                network = Network.get_network(network_name)
                protocol = Protocol.get_protocol(protocol_name)
                asset_instance, is_created = cls.objects.get_or_create(
                    asset=token_retriever.token_address,
                    network=network.id,
                    protocol=protocol.id
                )
                asset_instance.symbol = token_retriever.symbol
                asset_instance.num_decimals = token_retriever.num_decimals
                asset_instance.decimals = math.pow(10, token_retriever.num_decimals)
                asset_instance.save()

                serialized_value = serialize("json", [asset_instance])
                cache.set(key, serialized_value)
                return asset_instance

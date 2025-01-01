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

    decimals_price = models.DecimalField(
        max_digits=72, decimal_places=0, null=True, blank=True
    )

    max_cap = models.DecimalField(
        max_digits=72, decimal_places=0, null=True, blank=True
    )

    price = models.DecimalField(
        max_digits=72, decimal_places=10, null=True, blank=True
    )
    price_in_usdt = models.DecimalField(
        max_digits=72, decimal_places=2, null=True, blank=True
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

    def get_price(self):
        if self.price_type in (Asset.PriceType.CONSTANT, Asset.PriceType.AGGREGATOR):
            price = self.priceA
            price_in_usdt = price / self.decimals_price
            # normalize by decimal places to get USDT price on ARB
            return price, price_in_usdt

        elif self.price_type == Asset.PriceType.MAX_CAPPED:
            price = self.priceA
            if price > self.max_cap:
                price = self.max_cap
            price_in_usdt = price / self.decimals_price
            # Max Cap is stored in max_cap
            return price, price_in_usdt

        elif self.price_type == Asset.PriceType.RATIO:
            price = self.priceA
            ratio = self.priceB
            if ratio > self.max_cap:
                ratio = self.max_cap
            # current value of ratio is stored in priceB, and max
            # cap for ratio is stored in max_cap

            price = price * ratio
            price_in_usdt = price / self.decimals_price
            return price, price_in_usdt


class AssetPriceLog(models.Model):
    aggregator_address = models.CharField(max_length=42)
    network = models.ForeignKey('blockchains.Network', on_delete=models.PROTECT)
    price = models.DecimalField(max_digits=72, decimal_places=0, null=True, blank=True)
    onchain_created_at = models.DateTimeField()
    onchain_received_at = models.DateTimeField(null=True, blank=True)
    db_created_at = models.DateTimeField(auto_now_add=True)
    round_id = models.PositiveIntegerField(null=True, blank=True)
    provider = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        app_label = 'aave'
        indexes = [
            models.Index(fields=['aggregator_address', 'network']),
        ]

    def __str__(self):
        return f"{self.aggregator_address} price: {self.price} at {self.onchain_created_at}"


class AaveLiquidationLog(models.Model):
    network = models.ForeignKey('blockchains.Network', on_delete=models.PROTECT)
    protocol = models.ForeignKey('blockchains.Protocol', on_delete=models.PROTECT)

    user = models.CharField(max_length=42)
    debt_to_cover = models.DecimalField(max_digits=72, decimal_places=0, null=True, blank=True)
    debt_to_cover_in_usd = models.DecimalField(max_digits=70, decimal_places=2, null=True, blank=True)
    liquidated_collateral_amount = models.DecimalField(max_digits=72, decimal_places=0, null=True, blank=True)
    liquidated_collateral_amount_in_usd = models.DecimalField(max_digits=70, decimal_places=2, null=True, blank=True)

    collateral_asset = models.ForeignKey(
        'aave.Asset',
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name='collateral_liquidations'
    )
    debt_asset = models.ForeignKey(
        'aave.Asset',
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name='debt_liquidations'
    )

    liquidator = models.CharField(max_length=42)

    block_height = models.PositiveIntegerField(null=True, blank=True)
    transaction_hash = models.CharField(max_length=66, null=True, blank=True)
    transaction_index = models.PositiveIntegerField(null=True, blank=True)

    onchain_created_at = models.DateTimeField(null=True, blank=True)
    onchain_received_at = models.DateTimeField(null=True, blank=True)
    db_created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = 'Aave Liquidation Log'
        verbose_name_plural = 'Aave Liquidation Logs'
        indexes = [
            models.Index(fields=['network', 'protocol', 'user']),
        ]

    def __str__(self):
        return f"Liquidation {self.transaction_hash[:10]}... - {self.network}"

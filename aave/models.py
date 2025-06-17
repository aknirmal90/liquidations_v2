import logging
import math
from decimal import Decimal

from django.core.cache import cache
from django.core.serializers import deserialize, serialize
from django.db import models

from utils.tokens import EvmTokenRetriever

logger = logging.getLogger(__name__)


class Asset(models.Model):
    asset = models.CharField(max_length=255, null=False, blank=False)

    atoken_address = models.CharField(max_length=255, null=True, blank=True)
    stable_debt_token_address = models.CharField(max_length=255, null=True, blank=True)
    variable_debt_token_address = models.CharField(
        max_length=255, null=True, blank=True
    )

    collateral_liquidity_index = models.DecimalField(
        max_digits=72, decimal_places=0, default=Decimal("0.0")
    )
    borrow_liquidity_index = models.DecimalField(
        max_digits=72, decimal_places=0, default=Decimal("0.0")
    )

    symbol = models.CharField(max_length=255, null=True, blank=True)
    num_decimals = models.DecimalField(
        default=Decimal("0"), max_digits=3, decimal_places=0
    )
    decimals = models.DecimalField(
        max_digits=72, decimal_places=0, default=Decimal("0")
    )
    is_enabled = models.BooleanField(default=False)

    liquidation_threshold = models.DecimalField(
        max_digits=12, decimal_places=6, default=Decimal("0")
    )
    liquidation_bonus = models.DecimalField(
        max_digits=12, decimal_places=6, default=Decimal("0")
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
        CONSTANT = "constant"
        AGGREGATOR = "aggregator"
        MAX_CAPPED = "max_capped"
        RATIO = "ratio"

    PRICE_TYPE_CHOICES = [
        (PriceType.CONSTANT, "Constant"),
        (PriceType.AGGREGATOR, "Aggregator"),
        (PriceType.MAX_CAPPED, "Max Capped"),
        (PriceType.RATIO, "Ratio"),
    ]
    price_type = models.CharField(
        max_length=255, choices=PRICE_TYPE_CHOICES, null=True, blank=True
    )

    priceA = models.DecimalField(max_digits=72, decimal_places=0, null=True, blank=True)

    priceB = models.DecimalField(max_digits=72, decimal_places=0, null=True, blank=True)

    decimals_price = models.DecimalField(
        max_digits=72, decimal_places=0, null=True, blank=True
    )

    max_cap = models.DecimalField(
        max_digits=72, decimal_places=0, null=True, blank=True
    )

    price = models.DecimalField(max_digits=72, decimal_places=10, null=True, blank=True)
    price_in_nativeasset = models.DecimalField(
        max_digits=72, decimal_places=8, null=True, blank=True
    )

    emode_category = models.PositiveSmallIntegerField(default=0)

    def __str__(self) -> str:
        return f"{self.symbol}"

    class Meta:
        unique_together = ("network", "asset")
        app_label = "aave"

    @classmethod
    def _deserialize_from_cache(cls, key: str):
        """
        Internal helper for loading a single Asset from cache (serialized JSON).
        Returns None if not found, otherwise returns the deserialized Asset object.
        """
        serialized_value = cache.get(key)
        if serialized_value:
            return next(deserialize("json", serialized_value)).object
        return None

    @classmethod
    def _serialize_and_cache(cls, key: str, asset_instance):
        """
        Internal helper for serializing a single Asset instance to JSON
        and storing it in cache.
        Returns the asset_instance for convenience.
        """
        serialized_value = serialize("json", [asset_instance])
        cache.set(key, serialized_value)
        return asset_instance

    @classmethod
    def get_cache_key_by_address(cls, network_name: str, token_address: str):
        return f"asset-aave-{network_name}-{token_address}"

    @classmethod
    def get_cache_key_by_id(cls, id: int):
        return f"asset-aave-{id}"

    @classmethod
    def get_a_token_cache_key(cls, network_id: int, atoken_address: str):
        return f"asset-a-token-aave-{network_id}-{atoken_address}"

    @classmethod
    def get_by_address(cls, network_name: str, token_address: str):
        """
        Fetches an Asset by address and network from cache if possible,
        otherwise creates or loads from DB, then caches before returning.
        """
        if network_name is None or token_address is None:
            return None

        key = cls.get_cache_key_by_address(network_name, token_address)

        # Re-use our internal helper to attempt a cache load
        cached_asset = cls._deserialize_from_cache(key)
        if cached_asset:
            return cached_asset

        # Not in cache, so load from DB or create if needed
        try:
            token = cls.objects.get(
                asset__iexact=token_address,
                network__name=network_name,
            )
        except cls.DoesNotExist:
            # If not found, use EvmTokenRetriever to create the record
            token_retriever = EvmTokenRetriever(
                network_name=network_name, token_address=token_address
            )
            token, _ = cls.objects.get_or_create(
                asset=token_retriever.token_address,
            )
            token.symbol = token_retriever.symbol
            token.num_decimals = token_retriever.num_decimals
            token.decimals = math.pow(10, token_retriever.num_decimals)
            token.save()

        # Cache the found/created token and return
        return cls._serialize_and_cache(key, token)

    @classmethod
    def get_by_id(cls, id: int):
        """
        Fetches an Asset by primary key from cache if possible,
        otherwise loads from DB, then re-caches.
        """
        if id is None:
            return None

        key = cls.get_cache_key_by_id(id)
        cached_asset = cls._deserialize_from_cache(key)
        if cached_asset:
            return cached_asset

        # Not in cache, load from DB
        asset = cls.objects.get(id=id)
        return cls._serialize_and_cache(key, asset)

    def _clamp_value(self, value: Decimal, maximum: Decimal) -> Decimal:
        """Clamp the given value to the maximum cap if it exceeds it."""
        if maximum is not None and value > maximum:
            return maximum
        return value

    def _to_native_asset_price(self, price: Decimal) -> Decimal:
        """
        Convert a 'raw' price (an integer or big decimal) to a
        normalized price, dividing by self.decimals_price.
        """
        if not self.decimals_price or self.decimals_price == 0:
            return Decimal("0")
        return price / self.decimals_price

    def _compute_price_constant_or_aggregator(self) -> tuple[Decimal, Decimal]:
        """
        Logic for CONSTANT or AGGREGATOR price type:
         - We just return priceA as price
         - Then convert to native using decimals_price
        """
        if self.priceA is None:
            return None, None
        price = self.priceA
        price_in_nativeasset = self._to_native_asset_price(price)
        return price, price_in_nativeasset

    def _compute_price_max_capped(self) -> tuple[Decimal, Decimal]:
        """
        Logic for MAX_CAPPED price type:
         - Price is priceA, but clamped so it can't exceed max_cap
         - Then convert to native using decimals_price
        """
        if self.priceA is None or self.max_cap is None:
            return None, None
        price_clamped = self._clamp_value(self.priceA, self.max_cap)
        price_in_nativeasset = self._to_native_asset_price(price_clamped)
        return price_clamped, price_in_nativeasset

    def _compute_price_ratio(self) -> tuple[Decimal, Decimal]:
        """
        Logic for RATIO price type:
         - Price starts as priceA, multiplied by ratio from priceB
         - Ratio is clamped by self.max_cap
         - Then convert to native using decimals_price
        """
        if self.priceA is None or self.priceB is None or self.max_cap is None:
            return None, None
        ratio_clamped = self._clamp_value(self.priceB, self.max_cap)
        price = self.priceA * ratio_clamped
        price_in_nativeasset = self._to_native_asset_price(price)
        return price, price_in_nativeasset

    def get_price(self):
        """
        A refactored get_price method that delegates to smaller,
        more focused helper methods based on price_type.
        """
        # If there's no max_cap or relevant pricing data, or if price_type is None:
        if not self.price_type:
            return None, None

        handlers = {
            self.PriceType.CONSTANT: self._compute_price_constant_or_aggregator,
            self.PriceType.AGGREGATOR: self._compute_price_constant_or_aggregator,
            self.PriceType.MAX_CAPPED: self._compute_price_max_capped,
            self.PriceType.RATIO: self._compute_price_ratio,
        }

        # If the price_type is known, call its handler; else return None.
        handler = handlers.get(self.price_type)
        if handler:
            return handler()
        return None, None


class AssetPriceLog(models.Model):
    transaction_hash = models.CharField(max_length=66, null=True, blank=True)
    aggregator_address = models.CharField(max_length=42)
    network = models.ForeignKey("blockchains.Network", on_delete=models.PROTECT)
    price = models.DecimalField(max_digits=72, decimal_places=0, null=True, blank=True)
    onchain_created_at = models.DateTimeField(null=True, blank=True)
    onchain_received_at = models.DateTimeField(null=True, blank=True)
    processed_at = models.DateTimeField(null=True, blank=True)
    db_created_at = models.DateTimeField(auto_now_add=True)
    round_id = models.PositiveIntegerField(null=True, blank=True)
    provider = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        app_label = "aave"
        indexes = [
            models.Index(fields=["aggregator_address", "network"]),
        ]

    def __str__(self):
        return f"{self.aggregator_address} price: {self.price} at {self.onchain_created_at}"

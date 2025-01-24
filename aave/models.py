import logging
import math
from decimal import Decimal

from django.core.cache import cache
from django.core.serializers import deserialize, serialize
from django.db import models

from blockchains.models import Network
from utils.tokens import EvmTokenRetriever

logger = logging.getLogger(__name__)


class Asset(models.Model):

    asset = models.CharField(max_length=255, null=False, blank=False)
    network = models.ForeignKey(Network, on_delete=models.PROTECT, null=False)

    atoken_address = models.CharField(max_length=255, null=True, blank=True)
    stable_debt_token_address = models.CharField(max_length=255, null=True, blank=True)
    variable_debt_token_address = models.CharField(max_length=255, null=True, blank=True)

    collateral_liquidity_index = models.DecimalField(max_digits=72, decimal_places=0, default=Decimal("0.0"))
    borrow_liquidity_index = models.DecimalField(max_digits=72, decimal_places=0, default=Decimal("0.0"))

    symbol = models.CharField(max_length=255, null=True, blank=True)
    num_decimals = models.DecimalField(
        default=Decimal("0"), max_digits=3, decimal_places=0
    )
    decimals = models.DecimalField(max_digits=72, decimal_places=0, default=Decimal("0"))
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
    price_in_nativeasset = models.DecimalField(
        max_digits=72, decimal_places=8, null=True, blank=True
    )

    emode_category = models.PositiveSmallIntegerField(default=0)

    def __str__(self) -> str:
        return f"{self.symbol}"

    class Meta:
        unique_together = ('network', 'asset')
        app_label = 'aave'

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
            token_retriever = EvmTokenRetriever(network_name=network_name, token_address=token_address)
            network = Network.get_network_by_name(network_name)
            token, _ = cls.objects.get_or_create(
                asset=token_retriever.token_address,
                network=network.id,
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
    network = models.ForeignKey('blockchains.Network', on_delete=models.PROTECT)
    price = models.DecimalField(max_digits=72, decimal_places=0, null=True, blank=True)
    onchain_created_at = models.DateTimeField(null=True, blank=True)
    onchain_received_at = models.DateTimeField(null=True, blank=True)
    processed_at = models.DateTimeField(null=True, blank=True)
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

    user = models.CharField(max_length=42)
    debt_to_cover = models.DecimalField(max_digits=72, decimal_places=0, null=True, blank=True)
    debt_to_cover_in_usd = models.DecimalField(max_digits=70, decimal_places=2, null=True, blank=True)
    liquidated_collateral_amount = models.DecimalField(max_digits=72, decimal_places=0, null=True, blank=True)
    liquidated_collateral_amount_in_usd = models.DecimalField(max_digits=70, decimal_places=2, null=True, blank=True)
    profit_in_usd = models.DecimalField(max_digits=70, decimal_places=2, null=True, blank=True)

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

    block_datetime = models.DateTimeField(null=True, blank=True)
    block_height = models.PositiveIntegerField(null=True, blank=True)
    transaction_hash = models.CharField(max_length=66, null=True, blank=True)
    transaction_index = models.PositiveIntegerField(null=True, blank=True)

    onchain_created_at = models.DateTimeField(null=True, blank=True)
    onchain_received_at = models.DateTimeField(null=True, blank=True)
    db_created_at = models.DateTimeField(auto_now_add=True)

    health_factor_before_tx = models.DecimalField(
        max_digits=19,
        decimal_places=18,
        null=True,
        blank=True
    )
    health_factor_before_zero_blocks = models.DecimalField(
        max_digits=19,
        decimal_places=18,
        null=True,
        blank=True
    )
    health_factor_before_one_blocks = models.DecimalField(
        max_digits=19,
        decimal_places=18,
        null=True,
        blank=True
    )
    health_factor_before_two_blocks = models.DecimalField(
        max_digits=19,
        decimal_places=18,
        null=True,
        blank=True
    )
    health_factor_before_three_blocks = models.DecimalField(
        max_digits=19,
        decimal_places=18,
        null=True,
        blank=True
    )

    class Meta:
        verbose_name = 'Aave Liquidation Log'
        verbose_name_plural = 'Aave Liquidation Logs'
        indexes = [
            models.Index(fields=['network', 'user']),
        ]

    def __str__(self):
        return f"Liquidation {self.transaction_hash[:10]}... - {self.network}"


class AaveBalanceLog(models.Model):
    network = models.ForeignKey('blockchains.Network', on_delete=models.PROTECT)
    address = models.CharField(max_length=42, db_index=True)
    asset = models.ForeignKey('aave.Asset', on_delete=models.PROTECT)
    price_in_nativeasset = models.DecimalField(
        max_digits=72, decimal_places=8, null=True, blank=True
    )

    last_updated_collateral_liquidity_index = models.DecimalField(
        max_digits=72, decimal_places=0, default=Decimal("0.0")
    )
    last_updated_borrow_liquidity_index = models.DecimalField(max_digits=72, decimal_places=0, default=Decimal("0.0"))

    collateral_amount = models.DecimalField(max_digits=72, decimal_places=18, default=Decimal("0.0"))
    collateral_amount_live = models.DecimalField(max_digits=72, decimal_places=18, default=Decimal("0.0"))
    collateral_amount_live_with_liquidation_threshold = models.DecimalField(
        max_digits=72, decimal_places=18, default=Decimal("0.0")
    )
    collateral_amount_live_is_verified = models.BooleanField(default=None, null=True, blank=True)
    collateral_is_enabled = models.BooleanField(default=False)
    collateral_is_enabled_updated_at_block = models.PositiveBigIntegerField(default=0)
    collateral_health_factor = models.DecimalField(max_digits=72, decimal_places=0, default=Decimal("0.0"))

    borrow_amount = models.DecimalField(max_digits=72, decimal_places=18, default=Decimal("0.0"))
    borrow_amount_live = models.DecimalField(max_digits=72, decimal_places=18, default=Decimal("0.0"))
    borrow_amount_live_is_verified = models.BooleanField(default=None, null=True, blank=True)
    borrow_is_enabled = models.BooleanField(default=False)

    mark_for_deletion = models.BooleanField(default=False)

    emode_category = models.PositiveSmallIntegerField(default=0)
    emode_category_updated_at_block = models.PositiveBigIntegerField(default=0)

    user = models.ForeignKey('aave.AaveUser', on_delete=models.PROTECT, null=True, blank=True)

    class Meta:
        unique_together = ('network', 'address', 'asset')
        app_label = 'aave'
        indexes = [
            models.Index(fields=['network', 'address', 'asset']),
        ]

    def is_collateral_liquidity_index_updated(self):
        return self.last_updated_collateral_liquidity_index and self.asset.collateral_liquidity_index

    def is_borrow_liquidity_index_updated(self):
        return self.last_updated_borrow_liquidity_index and self.asset.borrow_liquidity_index

    def get_scaled_balance(self, type="collateral"):
        if type not in ["collateral", "borrow"]:
            raise ValueError(f"Invalid balance type: {type}")

        amount = self.collateral_amount if type == "collateral" else self.borrow_amount
        if not self.collateral_is_enabled:
            return Decimal("0.0")

        try:
            if type == "collateral":
                scale = self.asset.collateral_liquidity_index / self.last_updated_collateral_liquidity_index
            else:
                scale = self.asset.borrow_liquidity_index / self.last_updated_borrow_liquidity_index
        except Exception:
            return amount

        if type == "collateral":
            if not self.is_collateral_liquidity_index_updated():
                return amount.quantize(Decimal('1.00'))
        else:
            if not self.is_borrow_liquidity_index_updated():
                return amount.quantize(Decimal('1.00'))

        return (amount * scale).quantize(Decimal('1.00'))

    def get_unscaled_balance(self, amount, type="collateral"):
        if type == "collateral":
            if not self.is_collateral_liquidity_index_updated():
                return amount
        else:
            if not self.is_borrow_liquidity_index_updated():
                return amount

        if type == "collateral":
            scale = self.last_updated_collateral_liquidity_index / self.asset.collateral_liquidity_index
        else:
            scale = self.last_updated_borrow_liquidity_index / self.asset.borrow_liquidity_index

        return (amount * scale).quantize(Decimal('1.00'))


class AaveTransferEvent(models.Model):
    balance_log = models.ForeignKey('aave.AaveBalanceLog', on_delete=models.CASCADE)

    from_address = models.CharField(max_length=42, db_index=True)
    to_address = models.CharField(max_length=42, db_index=True)
    value = models.DecimalField(max_digits=72, decimal_places=18)
    block_height = models.PositiveBigIntegerField()
    transaction_hash = models.CharField(max_length=66)
    transaction_index = models.PositiveIntegerField()
    log_index = models.PositiveIntegerField()

    class Meta:
        verbose_name = 'Aave Transfer Event'
        verbose_name_plural = 'Aave Transfer Events'
        unique_together = ('transaction_hash', 'block_height', 'log_index', 'balance_log')


class AaveMintEvent(models.Model):
    balance_log = models.ForeignKey('aave.AaveBalanceLog', on_delete=models.CASCADE)

    TYPE_CHOICES = [
        ('collateral', 'Collateral'),
        ('borrow', 'Borrow')
    ]
    type = models.CharField(max_length=10, choices=TYPE_CHOICES, default='collateral')

    caller = models.CharField(max_length=42, db_index=True)
    on_behalf_of = models.CharField(max_length=42, db_index=True)
    value = models.DecimalField(max_digits=72, decimal_places=18)
    balance_increase = models.DecimalField(max_digits=72, decimal_places=18)
    index = models.DecimalField(max_digits=72, decimal_places=18)
    block_height = models.PositiveBigIntegerField()
    transaction_hash = models.CharField(max_length=66)
    transaction_index = models.PositiveIntegerField()
    log_index = models.PositiveIntegerField()

    class Meta:
        verbose_name = 'Aave Mint Event'
        verbose_name_plural = 'Aave Mint Events'
        unique_together = ('transaction_hash', 'block_height', 'log_index', 'balance_log', 'type')


class AaveBurnEvent(models.Model):
    balance_log = models.ForeignKey('aave.AaveBalanceLog', on_delete=models.CASCADE)

    TYPE_CHOICES = [
        ('collateral', 'Collateral'),
        ('borrow', 'Borrow')
    ]
    type = models.CharField(max_length=10, choices=TYPE_CHOICES, default='collateral')

    from_address = models.CharField(max_length=42, db_index=True)
    target = models.CharField(max_length=42, db_index=True)
    value = models.DecimalField(max_digits=72, decimal_places=18)
    balance_increase = models.DecimalField(max_digits=72, decimal_places=18)
    index = models.DecimalField(max_digits=72, decimal_places=18)
    block_height = models.PositiveBigIntegerField()
    transaction_hash = models.CharField(max_length=66)
    transaction_index = models.PositiveIntegerField()
    log_index = models.PositiveIntegerField()

    class Meta:
        verbose_name = 'Aave Burn Event'
        verbose_name_plural = 'Aave Burn Events'
        unique_together = ('transaction_hash', 'block_height', 'log_index', 'balance_log', 'type')


class AaveSupplyEvent(models.Model):
    balance_log = models.ForeignKey('aave.AaveBalanceLog', on_delete=models.CASCADE)

    user = models.CharField(max_length=42, db_index=True)
    on_behalf_of = models.CharField(max_length=42, db_index=True)
    amount = models.DecimalField(max_digits=72, decimal_places=18)
    referral_code = models.PositiveSmallIntegerField()

    block_height = models.PositiveBigIntegerField()
    transaction_hash = models.CharField(max_length=66)
    transaction_index = models.PositiveIntegerField()
    log_index = models.PositiveIntegerField()

    class Meta:
        verbose_name = 'Aave Supply Event'
        verbose_name_plural = 'Aave Supply Events'
        unique_together = ('transaction_hash', 'block_height', 'log_index', 'balance_log')


class AaveWithdrawEvent(models.Model):
    balance_log = models.ForeignKey('aave.AaveBalanceLog', on_delete=models.CASCADE)

    user = models.CharField(max_length=42, db_index=True)
    to_address = models.CharField(max_length=42, db_index=True)
    amount = models.DecimalField(max_digits=72, decimal_places=18)

    block_height = models.PositiveBigIntegerField()
    transaction_hash = models.CharField(max_length=66)
    transaction_index = models.PositiveIntegerField()
    log_index = models.PositiveIntegerField()

    class Meta:
        verbose_name = 'Aave Withdraw Event'
        verbose_name_plural = 'Aave Withdraw Events'
        unique_together = ('transaction_hash', 'block_height', 'log_index', 'balance_log')


class AaveDataQualityAnalyticsReport(models.Model):
    network = models.ForeignKey('blockchains.Network', on_delete=models.PROTECT)
    date = models.DateField(db_index=True)

    num_collateral_verified = models.PositiveIntegerField(default=0)
    num_borrow_verified = models.PositiveIntegerField(default=0)
    num_collateral_unverified = models.PositiveIntegerField(default=0)
    num_borrow_unverified = models.PositiveIntegerField(default=0)
    num_collateral_deleted = models.PositiveIntegerField(default=0)
    num_borrow_deleted = models.PositiveIntegerField(default=0)

    def __str__(self):
        return f"{self.network} - {self.date}"


class AaveBorrowEvent(models.Model):
    balance_log = models.ForeignKey('aave.AaveBalanceLog', on_delete=models.CASCADE)

    user = models.CharField(max_length=42, db_index=True)
    on_behalf_of = models.CharField(max_length=42, db_index=True)
    amount = models.DecimalField(max_digits=72, decimal_places=18)
    interest_rate_mode = models.PositiveSmallIntegerField()
    borrow_rate = models.DecimalField(max_digits=72, decimal_places=18)
    referral_code = models.PositiveSmallIntegerField()

    block_height = models.PositiveBigIntegerField()
    transaction_hash = models.CharField(max_length=66)
    transaction_index = models.PositiveIntegerField()
    log_index = models.PositiveIntegerField()

    class Meta:
        verbose_name = 'Aave Borrow Event'
        verbose_name_plural = 'Aave Borrow Events'
        unique_together = ('transaction_hash', 'block_height', 'log_index', 'balance_log')


class AaveRepayEvent(models.Model):
    balance_log = models.ForeignKey('aave.AaveBalanceLog', on_delete=models.CASCADE)

    user = models.CharField(max_length=42, db_index=True)
    repayer = models.CharField(max_length=42, db_index=True)
    amount = models.DecimalField(max_digits=72, decimal_places=18)
    use_a_tokens = models.BooleanField()

    block_height = models.PositiveBigIntegerField()
    transaction_hash = models.CharField(max_length=66)
    transaction_index = models.PositiveIntegerField()
    log_index = models.PositiveIntegerField()

    class Meta:
        verbose_name = 'Aave Repay Event'
        verbose_name_plural = 'Aave Repay Events'
        unique_together = ('transaction_hash', 'block_height', 'log_index', 'balance_log')


class AaveLiquidationCallEvent(models.Model):
    balance_log = models.ForeignKey('aave.AaveBalanceLog', on_delete=models.CASCADE)

    collateral_asset = models.CharField(max_length=42, db_index=True)
    debt_asset = models.CharField(max_length=42, db_index=True)
    user = models.CharField(max_length=42, db_index=True)
    debt_to_cover = models.DecimalField(max_digits=72, decimal_places=18)
    liquidated_collateral_amount = models.DecimalField(max_digits=72, decimal_places=18)
    liquidator = models.CharField(max_length=42, db_index=True)
    receive_a_token = models.BooleanField()

    block_height = models.PositiveBigIntegerField()
    transaction_hash = models.CharField(max_length=66)
    transaction_index = models.PositiveIntegerField()
    log_index = models.PositiveIntegerField()

    class Meta:
        verbose_name = 'Aave Liquidation Call Event'
        verbose_name_plural = 'Aave Liquidation Call Events'
        unique_together = ('transaction_hash', 'block_height', 'log_index', 'balance_log')


class AaveUser(models.Model):
    network = models.ForeignKey('blockchains.Network', on_delete=models.PROTECT)
    address = models.CharField(max_length=42, db_index=True)
    health_factor = models.DecimalField(max_digits=21, decimal_places=18, null=True, blank=True)

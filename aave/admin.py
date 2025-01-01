from django.contrib import admin

from aave.models import AaveLiquidationLog, Asset, AssetPriceLog
from utils.admin import EnableDisableAdminMixin, get_explorer_address_url, get_explorer_transaction_url


@admin.register(Asset)
class AssetAdmin(EnableDisableAdminMixin, admin.ModelAdmin):
    list_display = (
        'symbol',
        'protocol',
        'network',
        'get_asset_link',
        'is_enabled',
        'get_pricesource_link',
        'reserve_is_frozen',
        'reserve_is_borrow_enabled',
        'emode_category',
        'emode_is_collateral',
        'emode_is_borrowable',
        'price_in_usdt',
        'is_reserve_paused',
        'priceA',
        'priceB',
        'updated_at_block_heightA',
        'updated_at_block_heightB',
    )
    list_filter = (
        'protocol',
        'network',
        'is_enabled',
        'emode_category',
        'is_reserve_paused',
        'borrowable_in_isolation_mode',
        'reserve_is_frozen',
        'reserve_is_borrow_enabled',
        'reserve_is_flash_loan_enabled',
        'emode_is_collateral',
        'emode_is_borrowable',
        'price_type',
    )

    fieldsets = (
        ('Status', {
            'fields': (
                'is_enabled',
                'is_reserve_paused',
                'reserve_is_frozen',
                'reserve_is_borrow_enabled',
                'reserve_is_flash_loan_enabled'
            )
        }),
        ('Asset Information', {
            'fields': (
                ('symbol', 'protocol', 'network'),
                ('decimals', 'num_decimals'),
                'get_asset_link'
            )
        }),
        ('Associated Token Addresses', {
            'fields': (
                'get_atoken_address_link',
                'get_stable_debt_token_address_link',
                'get_variable_debt_token_address_link',
                'get_interest_rate_strategy_address_link'
            )
        }),
        ('Price Information', {
            'fields': (
                ('price', 'price_in_usdt'),
                ('get_pricesource_link', 'price_type'),
                ('get_contractA_link', 'priceA', 'decimals_price', 'updated_at_block_heightA'),
                ('get_contractB_link', 'priceB', 'max_cap', 'updated_at_block_heightB')
            )
        }),
        ('Risk Parameters', {
            'fields': (
                'reserve_factor',
                'borrowable_in_isolation_mode',
                ('liquidation_threshold', 'liquidation_bonus')
            )
        }),
        ('E-Mode Configuration', {
            'fields': (
                'emode_category',
                ('emode_is_collateral', 'emode_is_borrowable'),
                ('emode_liquidation_threshold', 'emode_liquidation_bonus')
            )
        }),
    )

    readonly_fields = (
        'symbol',
        'protocol',
        'network',
        'decimals',
        'num_decimals',
        'get_asset_link',
        'get_atoken_address_link',
        'get_stable_debt_token_address_link',
        'get_variable_debt_token_address_link',
        'get_interest_rate_strategy_address_link',
        'liquidation_threshold',
        'liquidation_bonus',
        'emode_liquidation_threshold',
        'emode_liquidation_bonus',
        'emode_category',
        'get_pricesource_link',
        'get_contractA_link',
        'get_contractB_link',
        'priceA',
        'priceB',
        'decimals_price',
        'max_cap',
        'price',
        'price_in_usdt',
        'updated_at_block_heightA',
        'updated_at_block_heightB',
        'price_type',
        'borrowable_in_isolation_mode',
        'reserve_factor',
        'reserve_is_flash_loan_enabled',
        'reserve_is_borrow_enabled',
        'reserve_is_frozen',
        'emode_is_collateral',
        'emode_is_borrowable',
        'is_reserve_paused'
    )

    def get_asset_link(self, obj):
        return get_explorer_address_url(obj.network, obj.asset)
    get_asset_link.short_description = "Asset"

    def get_atoken_address_link(self, obj):
        return get_explorer_address_url(obj.network, obj.atoken_address)
    get_atoken_address_link.short_description = "aToken Address"

    def get_stable_debt_token_address_link(self, obj):
        return get_explorer_address_url(obj.network, obj.stable_debt_token_address)
    get_stable_debt_token_address_link.short_description = "Stable Debt Token Address"

    def get_variable_debt_token_address_link(self, obj):
        return get_explorer_address_url(obj.network, obj.variable_debt_token_address)
    get_variable_debt_token_address_link.short_description = "Variable Debt Token Address"

    def get_interest_rate_strategy_address_link(self, obj):
        return get_explorer_address_url(obj.network, obj.interest_rate_strategy_address)
    get_interest_rate_strategy_address_link.short_description = "Interest Rate Strategy Address"

    def get_contractA_link(self, obj):
        return get_explorer_address_url(obj.network, obj.contractA)
    get_contractA_link.short_description = "Contract A"

    def get_contractB_link(self, obj):
        return get_explorer_address_url(obj.network, obj.contractB)
    get_contractB_link.short_description = "Contract B"

    def get_pricesource_link(self, obj):
        return get_explorer_address_url(obj.network, obj.pricesource)
    get_pricesource_link.short_description = "Price Source"


@admin.register(AssetPriceLog)
class AssetPriceLogAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'get_aggregator_address_link',
        'provider',
        'network',
        'price',
        'round_id',
        'onchain_created_at',
        'db_created_at',
        'get_rpc_latency_ms',
        'get_db_latency_ms'
    )
    list_filter = ('network', 'onchain_created_at', 'db_created_at')
    search_fields = ('aggregator_address', 'round_id')
    ordering = ('-db_created_at',)

    def get_aggregator_address_link(self, obj):
        return get_explorer_address_url(obj.network, obj.aggregator_address)
    get_aggregator_address_link.short_description = "Asset Address"

    def get_rpc_latency_ms(self, obj):
        if obj.onchain_received_at:
            delta = obj.onchain_received_at - obj.onchain_created_at
            return int(delta.total_seconds() * 1000)
        return None
    get_rpc_latency_ms.short_description = "RPC Latency (ms)"

    def get_db_latency_ms(self, obj):
        if obj.onchain_received_at:
            delta = obj.db_created_at - obj.onchain_received_at
            return int(delta.total_seconds() * 1000)
    get_db_latency_ms.short_description = "DB Latency (ms)"

    readonly_fields = (
        'db_created_at',
        'get_aggregator_address_link',
        'aggregator_address',
        'network',
        'price',
        'round_id',
        'onchain_created_at',
        'onchain_received_at',
        'get_rpc_latency_ms',
        'get_db_latency_ms',
        'id'
    )

    fieldsets = (
        ('Asset Information', {
            'fields': (
                'aggregator_address',
                'provider',
                'get_aggregator_address_link',
                'network'
            )
        }),
        ('Price Information', {
            'fields': (
                'price',
                'round_id'
            )
        }),
        ('Timestamps', {
            'fields': (
                'onchain_created_at',
                'db_created_at',
                'onchain_received_at',
                'get_rpc_latency_ms',
                'get_db_latency_ms'
            )
        })
    )


@admin.register(AaveLiquidationLog)
class AaveLiquidationLogAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'get_transaction_hash_link',
        'network',
        'protocol',
        'get_liquidator_link',
        'collateral_asset',
        'debt_asset',
        'db_created_at',
        'debt_to_cover_in_usd',
        'liquidated_collateral_amount_in_usd'
    )

    list_filter = (
        'network',
        'protocol',
        'collateral_asset',
        'debt_asset',
        'db_created_at',
    )

    search_fields = (
        'transaction_hash',
        'user',
        'liquidator',
        'collateral_asset__symbol',
        'debt_asset__symbol',
    )

    fieldsets = (
        ('Transaction Information', {
            'fields': (
                'get_transaction_hash_link',
                'block_height',
                'transaction_index',
                'network',
                'protocol',
            )
        }),
        ('Addresses', {
            'fields': (
                'user',
                'get_liquidator_link',
            )
        }),
        ('Assets and Amounts', {
            'fields': (
                ('collateral_asset', 'liquidated_collateral_amount', 'liquidated_collateral_amount_in_usd'),
                ('debt_asset', 'debt_to_cover', 'debt_to_cover_in_usd'),
            )
        }),
        ('Timestamps', {
            'fields': (
                'onchain_created_at',
                'onchain_received_at',
                'db_created_at',
            )
        }),
    )

    readonly_fields = (
        'transaction_hash',
        'block_height',
        'transaction_index',
        'network',
        'protocol',
        'user',
        'liquidator',
        'collateral_asset',
        'liquidated_collateral_amount',
        'liquidated_collateral_amount_in_usd',
        'debt_asset',
        'debt_to_cover',
        'debt_to_cover_in_usd',
        'onchain_created_at',
        'onchain_received_at',
        'db_created_at',
        'get_liquidator_link',
        'get_transaction_hash_link',
        'id'
    )

    ordering = ('-block_height', '-transaction_index',)

    def get_transaction_hash_link(self, obj):
        return get_explorer_transaction_url(obj.network, obj.transaction_hash)
    get_transaction_hash_link.short_description = 'Transaction Hash'

    def get_liquidator_link(self, obj):
        return get_explorer_address_url(obj.network, obj.liquidator)
    get_liquidator_link.short_description = 'Liquidator'

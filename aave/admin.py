from decimal import Decimal

from django.contrib import admin
from django.db import models
from django.utils.safestring import mark_safe
from django_object_actions import DjangoObjectActions, action

from aave.dataprovider import AaveDataProvider
from aave.inlines import (
    AaveBurnEventInline,
    AaveMintEventInline,
    AaveSupplyEventInline,
    AaveTransferEventInline,
    AaveWithdrawEventInline,
    get_burn_events_for_address,
    get_mint_events_for_address,
    get_supply_events_for_address,
    get_transfer_events_for_address,
    get_withdraw_events_for_address,
)
from aave.models import AaveBalanceLog, AaveLiquidationLog, Asset, AssetPriceLog
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
        'liquidity_index',
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
                'reserve_is_flash_loan_enabled',
            )
        }),
        ('Asset Information', {
            'fields': (
                ('symbol', 'protocol', 'network'),
                ('decimals', 'num_decimals'),
                'get_asset_link',
                'liquidity_index'
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
                ('get_contractA_link', 'priceA', 'decimals_price'),
                ('get_contractB_link', 'priceB', 'max_cap')
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
        'get_rpc_latency_ms',
        'get_parsing_latency_ms',
        'get_celery_latency_ms'
    )
    list_filter = ('network', 'onchain_created_at', 'db_created_at', 'provider')
    search_fields = ('aggregator_address', 'round_id')
    ordering = ('-db_created_at',)

    def get_aggregator_address_link(self, obj):
        return get_explorer_address_url(obj.network, obj.aggregator_address)
    get_aggregator_address_link.short_description = "Asset Address"

    def get_rpc_latency_ms(self, obj):
        if obj.onchain_received_at and obj.onchain_created_at:
            delta = obj.onchain_received_at - obj.onchain_created_at
            return int(delta.total_seconds() * 1000)
        return None
    get_rpc_latency_ms.short_description = "RPC Latency (ms)"

    def get_parsing_latency_ms(self, obj):
        if obj.processed_at and obj.onchain_received_at:
            delta = obj.processed_at - obj.onchain_received_at
            return int(delta.total_seconds() * 1000)
        return None
    get_parsing_latency_ms.short_description = "Parsing Latency (ms)"

    def get_celery_latency_ms(self, obj):
        if obj.processed_at:
            delta = obj.db_created_at - obj.processed_at
            return int(delta.total_seconds() * 1000)
    get_celery_latency_ms.short_description = "Celery Latency (ms)"

    readonly_fields = (
        'db_created_at',
        'get_aggregator_address_link',
        'aggregator_address',
        'network',
        'price',
        'round_id',
        'onchain_created_at',
        'onchain_received_at',
        'processed_at',
        'get_rpc_latency_ms',
        'get_parsing_latency_ms',
        'get_celery_latency_ms',
        'id',
        'provider',
    )

    fieldsets = (
        ('Asset Information', {
            'fields': (
                'aggregator_address',
                'provider',
                'get_aggregator_address_link',
                'network',
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
                'processed_at',
                'get_rpc_latency_ms',
                'get_parsing_latency_ms',
                'get_celery_latency_ms'
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
        'block_height',
        'transaction_index',
        'get_liquidator_link',
        'collateral_asset',
        'debt_asset',
        'debt_to_cover_in_usd',
        'liquidated_collateral_amount_in_usd',
        'user',
        'health_factor_t',
        'health_factor_t0',
        'health_factor_t1',
        'health_factor_t2',
        'health_factor_t3'
    )

    def health_factor_t(self, obj):
        if obj.health_factor_before_tx is None:
            return None
        return obj.health_factor_before_tx < Decimal('1.00')
    health_factor_t.boolean = True
    health_factor_t.short_description = 'T'

    def health_factor_t0(self, obj):
        if obj.health_factor_before_zero_blocks is None:
            return None
        return obj.health_factor_before_zero_blocks < Decimal('1.00')
    health_factor_t0.boolean = True
    health_factor_t0.short_description = 'T-0'

    def health_factor_t1(self, obj):
        if obj.health_factor_before_one_blocks is None:
            return None
        return obj.health_factor_before_one_blocks < Decimal('1.00')
    health_factor_t1.boolean = True
    health_factor_t1.short_description = 'T-1'

    def health_factor_t2(self, obj):
        if obj.health_factor_before_two_blocks is None:
            return None
        return obj.health_factor_before_two_blocks < Decimal('1.00')
    health_factor_t2.boolean = True
    health_factor_t2.short_description = 'T-2'

    def health_factor_t3(self, obj):
        if obj.health_factor_before_three_blocks is None:
            return None
        return obj.health_factor_before_three_blocks < Decimal('1.00')
    health_factor_t3.boolean = True
    health_factor_t3.short_description = 'T-3'

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
        ('Simulations', {
            'fields': (
                'health_factor_before_tx',
                'health_factor_before_zero_blocks',
                'health_factor_before_one_blocks',
                'health_factor_before_two_blocks',
                'health_factor_before_three_blocks',
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
        'id',
        'health_factor_before_tx',
        'health_factor_before_zero_blocks',
        'health_factor_before_one_blocks',
        'health_factor_before_two_blocks',
        'health_factor_before_three_blocks',
    )

    ordering = ('-block_height', '-transaction_index',)

    def get_transaction_hash_link(self, obj):
        return get_explorer_transaction_url(obj.network, obj.transaction_hash)
    get_transaction_hash_link.short_description = 'Transaction Hash'

    def get_liquidator_link(self, obj):
        return get_explorer_address_url(obj.network, obj.liquidator)
    get_liquidator_link.short_description = 'Liquidator'


@admin.register(AaveBalanceLog)
class AaveBalanceLogAdmin(DjangoObjectActions, admin.ModelAdmin):
    list_display = (
        'id',
        'network',
        'protocol',
        'get_address_link',
        'asset',
        'collateral_amount',
        'collateral_amount_live_is_verified',
        'borrow_amount',
        'borrow_is_enabled'
    )

    list_filter = (
        'network',
        'protocol',
        'collateral_is_enabled',
        'borrow_is_enabled',
        'collateral_amount_live_is_verified',
        'mark_for_deletion'
    )

    search_fields = (
        'address',
        'asset__symbol'
    )

    readonly_fields = (
        'id',
        'network',
        'protocol',
        'get_address_link',
        'address',
        'asset',
        'last_updated_liquidity_index',
        'collateral_amount',
        'collateral_amount_live',
        'collateral_is_enabled',
        'borrow_amount',
        'borrow_is_enabled',
        'get_collateral_amount_contract',
        'collateral_amount_live_is_verified',
        'get_aggregate_amounts'
    )

    fieldsets = (
        ('Account Information', {
            'fields': (
                'get_address_link',
                'network',
                'protocol',
                'asset',
                'last_updated_liquidity_index'
            )
        }),
        ('Collateral Details', {
            'fields': (
                'collateral_amount',
                'collateral_amount_live',
                'get_collateral_amount_contract',
                'collateral_is_enabled',
                'collateral_amount_live_is_verified'
            )
        }),
        ('Borrow Details', {
            'fields': (
                'borrow_amount',
                'borrow_is_enabled'
            )
        }),
        ('Aggregate Event Details', {
            'fields': (
                'get_aggregate_amounts',
            )
        })
    )

    inlines = [
        AaveMintEventInline,
        AaveBurnEventInline,
        AaveTransferEventInline,
        AaveSupplyEventInline,
        AaveWithdrawEventInline,
    ]

    def get_address_link(self, obj):
        return get_explorer_address_url(obj.network, obj.address)
    get_address_link.short_description = 'Address'

    @action(label="Get Logs")
    def get_logs(self, request, obj):
        self.message_user(request, "Starting to fetch logs...")
        try:
            get_burn_events_for_address(obj)
            self.message_user(request, "Successfully fetched burn events")

            get_mint_events_for_address(obj)
            self.message_user(request, "Successfully fetched mint events")

            get_transfer_events_for_address(obj)
            self.message_user(request, "Successfully fetched transfer events")

            get_supply_events_for_address(obj)
            self.message_user(request, "Successfully fetched supply events")

            get_withdraw_events_for_address(obj)
            self.message_user(request, "Successfully fetched withdraw events")

            self.message_user(request, "Successfully fetched all logs")
        except Exception as e:
            self.message_user(request, f"Error fetching logs: {str(e)}", level='ERROR')

    change_actions = ('get_logs', )

    def get_collateral_amount_contract(self, obj):
        provider = AaveDataProvider(obj.network)
        user_reserve = provider.getUserReserveData(obj.asset.asset, [obj.address])[0]['result']
        collateral_amount_contract = Decimal(user_reserve.currentATokenBalance)
        if obj.collateral_amount_live:
            difference = obj.collateral_amount_live - collateral_amount_contract
        else:
            difference = ''

        if difference and collateral_amount_contract != Decimal('0'):
            pct_difference = (difference / collateral_amount_contract) * Decimal('10000.000000')
        else:
            pct_difference = ''

        # Determine color for the difference
        color = 'green' if difference and difference >= 0 else 'red'
        return mark_safe(
            '''
            <div>
                <div>Contract: <span style="color: blue">{}</span></div>
                <div>Difference: <span style="color: {}">{:+}</span></div>
                <div>% Difference: <span style="color: {}">{:+.6f} bps</span></div>
            </div>
            '''.format(
                collateral_amount_contract,
                color,
                difference,
                color,
                pct_difference
            )
        )
    get_collateral_amount_contract.short_description = 'Collateral Amount Contract'

    def get_aggregate_amounts(self, obj):
        total_mint = (
            obj.aavemintevent_set.aggregate(
                models.Sum('value'))['value__sum'] or Decimal('0')
        )
        total_mint_interest = (
            obj.aavemintevent_set.aggregate(
                models.Sum('balance_increase'))['balance_increase__sum'] or Decimal('0')
        )
        total_burn = (
            obj.aaveburnevent_set.aggregate(
                models.Sum('value'))['value__sum'] or Decimal('0')
        )
        total_burn_interest = (
            obj.aaveburnevent_set.aggregate(
                models.Sum('balance_increase'))['balance_increase__sum'] or Decimal('0')
        )
        total_transfer_in = (
            obj.aavetransferevent_set.filter(
                to_address__iexact=obj.address
            ).aggregate(models.Sum('value'))['value__sum'] or Decimal('0')
        )
        total_transfer_out = (
            obj.aavetransferevent_set.filter(
                from_address__iexact=obj.address
            ).aggregate(models.Sum('value'))['value__sum'] or Decimal('0')
        )
        total_supply = (
            obj.aavesupplyevent_set.aggregate(
                models.Sum('amount'))['amount__sum'] or Decimal('0')
        )
        total_withdraw = (
            obj.aavewithdrawevent_set.aggregate(
                models.Sum('amount'))['amount__sum'] or Decimal('0')
        )

        mint_burn_diff = total_mint - total_burn + total_transfer_in - total_transfer_out
        mint_burn_interest_diff = total_mint_interest - total_burn_interest

        return mark_safe(
            '''
            <div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
                <div>Total Mint: <span style="color: green">{:+}</span>
                (Interest: <span style="color: green">{:+}</span>)</div>
                <div>Total Burn: <span style="color: red">{:+}</span>
                (Interest: <span style="color: red">{:+}</span>)</div>
                <div>Total Transfer In: <span style="color: green">{:+}</span></div>
                <div>Total Transfer Out: <span style="color: red">{:+}</span></div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
                <div>Token Model Balance: <span style="color: {};">{:+}</span>
                (Interest: <span style="color: {};">{:+}</span>)</div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
                <div>Total Supply: <span style="color: green">{:+}</span></div>
                <div>Total Withdraw: <span style="color: red">{:+}</span></div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
            </div>
            '''.format(
                total_mint,
                total_mint_interest,
                total_burn,
                total_burn_interest,
                total_transfer_in,
                total_transfer_out,
                'green' if mint_burn_diff >= 0 else 'red',
                mint_burn_diff,
                'green' if mint_burn_interest_diff >= 0 else 'red',
                mint_burn_interest_diff,
                total_supply,
                total_withdraw
            )
        )
    get_aggregate_amounts.short_description = 'Aggregate Event Amounts'

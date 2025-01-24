from decimal import Decimal

from django.contrib import admin
from django.db import models
from django.utils.safestring import mark_safe
from django_object_actions import DjangoObjectActions, action

from aave.dataprovider import AaveDataProvider
from aave.inlines import (
    AaveBorrowEventInline,
    AaveBurnEventInline,
    AaveLiquidationCallEventInline,
    AaveMintEventInline,
    AaveRepayEventInline,
    AaveSupplyEventInline,
    AaveTransferEventInline,
    AaveWithdrawEventInline,
    get_borrow_events_for_address,
    get_burn_events_for_address,
    get_liquidation_call_events_for_address,
    get_mint_events_for_address,
    get_repay_events_for_address,
    get_supply_events_for_address,
    get_transfer_events_for_address,
    get_withdraw_events_for_address,
)
from aave.models import AaveBalanceLog, AaveDataQualityAnalyticsReport, AaveLiquidationLog, Asset, AssetPriceLog
from utils.admin import (
    EnableDisableAdminMixin,
    format_pretty_json,
    get_explorer_address_url,
    get_explorer_transaction_url,
)


@admin.register(Asset)
class AssetAdmin(EnableDisableAdminMixin, admin.ModelAdmin):
    list_display = (
        'symbol',
        'network',
        'get_asset_link',
        'is_enabled',
        'get_pricesource_link',
        'emode_category',
        'price_in_nativeasset',
        'priceA',
        'priceB',
        'liquidation_threshold',
        'liquidation_bonus',
        'emode_liquidation_threshold',
        'emode_liquidation_bonus',
    )
    list_filter = (
        'network',
        'is_enabled',
        'emode_category',
        'price_type',
    )

    fieldsets = (
        ('Asset Information', {
            'fields': (
                ('symbol', 'network'),
                'is_enabled',
                ('decimals', 'num_decimals'),
                'get_asset_link',
                ('collateral_liquidity_index', 'borrow_liquidity_index')
            )
        }),
        ('Associated Token Addresses', {
            'fields': (
                'get_atoken_address_link',
                'get_stable_debt_token_address_link',
                'get_variable_debt_token_address_link',
            )
        }),
        ('Price Information', {
            'fields': (
                ('price', 'price_in_nativeasset'),
                ('get_pricesource_link', 'price_type'),
                ('get_contractA_link', 'priceA', 'decimals_price'),
                ('get_contractB_link', 'priceB', 'max_cap')
            )
        }),
        ('Risk Parameters', {
            'fields': (
                ('liquidation_threshold', 'liquidation_bonus'),
                'emode_category',
                ('emode_liquidation_threshold', 'emode_liquidation_bonus')
            )
        }),
    )

    readonly_fields = (
        'symbol',
        'network',
        'decimals',
        'num_decimals',
        'get_asset_link',
        'get_atoken_address_link',
        'get_stable_debt_token_address_link',
        'get_variable_debt_token_address_link',
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
        'price_in_nativeasset',
        'price_type',
        'borrow_liquidity_index',
        'collateral_liquidity_index'
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
        'block_datetime',
        'transaction_index',
        'get_liquidator_link',
        'collateral_asset',
        'debt_asset',
        'profit_in_usd',
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
                'block_datetime',
                'block_height',
                'transaction_index',
                'network',
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
                ('profit_in_usd'),
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
        'block_datetime',
        'block_height',
        'transaction_index',
        'network',
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
        'profit_in_usd'
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
        'get_address_link',
        'asset',
        'collateral_amount',
        'borrow_amount',
        'collateral_health_factor',
        'price_in_nativeasset'
    )

    list_filter = (
        'network',
        'collateral_is_enabled',
        'borrow_is_enabled',
        'collateral_amount_live_is_verified',
        'borrow_amount_live_is_verified',
        'mark_for_deletion'
    )

    search_fields = (
        'address',
        'asset__symbol'
    )

    readonly_fields = (
        'id',
        'network',
        'get_address_link',
        'address',
        'asset',
        'last_updated_collateral_liquidity_index',
        'last_updated_borrow_liquidity_index',
        'collateral_amount',
        'collateral_amount_live',
        'collateral_is_enabled',
        'borrow_amount',
        'borrow_is_enabled',
        'get_collateral_amount_contract',
        'collateral_amount_live_is_verified',
        'get_collateral_aggregate_amounts',
        'borrow_amount_live',
        'borrow_amount_live_is_verified',
        'get_borrow_amount_contract',
        'get_borrow_aggregate_amounts',
        'get_user_reserve_data',
        'get_collateral_indexes',
        'get_borrow_indexes',
        'collateral_is_enabled_updated_at_block',
        'collateral_is_enabled',
        'price_in_nativeasset',
        'collateral_health_factor'
    )

    fieldsets = (
        ('Account Information', {
            'fields': (
                'get_address_link',
                'network',
                'asset',
                ('get_collateral_indexes', 'get_borrow_indexes'),
                ('collateral_is_enabled_updated_at_block', 'collateral_is_enabled'),
            )
        }),
        ('Collateral Details', {
            'fields': (
                'collateral_amount',
                'collateral_amount_live',
                'get_collateral_amount_contract',
                'collateral_amount_live_is_verified'
            )
        }),
        ('Borrow Details', {
            'fields': (
                'borrow_amount',
                'borrow_amount_live',
                'get_borrow_amount_contract',
                'borrow_amount_live_is_verified'
            )
        }),
        ('Aggregate Event Details', {
            'fields': (
                'get_collateral_aggregate_amounts',
                'get_borrow_aggregate_amounts',
            )
        }),
        ('User Reserve Data', {
            'fields': (
                'get_user_reserve_data',
            )
        })
    )

    inlines = [
        AaveMintEventInline,
        AaveBurnEventInline,
        AaveTransferEventInline,
        AaveSupplyEventInline,
        AaveWithdrawEventInline,
        AaveBorrowEventInline,
        AaveRepayEventInline,
        AaveLiquidationCallEventInline,
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

            get_liquidation_call_events_for_address(obj)
            self.message_user(request, "Successfully fetched liquidation call events")

            get_borrow_events_for_address(obj)
            self.message_user(request, "Successfully fetched borrow events")

            get_repay_events_for_address(obj)
            self.message_user(request, "Successfully fetched repay events")

            self.message_user(request, "Successfully fetched all logs")
        except Exception as e:
            self.message_user(request, f"Error fetching logs: {str(e)}", level='ERROR')

    change_actions = ('get_logs', )

    def get_collateral_amount_contract(self, obj):
        provider = AaveDataProvider(obj.network)
        user_reserve = provider.getUserReserveData(obj.asset.asset, [obj.address])[0]['result']
        collateral_amount_contract = Decimal(user_reserve.currentATokenBalance) / obj.asset.decimals
        collateral_amount_live = obj.get_scaled_balance("collateral")

        if collateral_amount_live:
            difference = collateral_amount_live - collateral_amount_contract
        else:
            difference = 0

        if difference and collateral_amount_contract != Decimal('0'):
            pct_difference = (difference / collateral_amount_contract) * Decimal('10000.000000')
        else:
            pct_difference = 0

        # Determine color for the difference
        color = 'green' if difference and difference >= 0 else 'red'
        return mark_safe(
            '''
            <div>
                <div>Contract: <span style="color: blue">{}</span></div>
                <div>Live: <span style="color: blue">{}</span></div>
                <div>Difference: <span style="color: {}">{:+}</span></div>
                <div>% Difference: <span style="color: {}">{:+.6f} bps</span></div>
            </div>
            '''.format(
                collateral_amount_contract,
                collateral_amount_live or 0,
                color,
                difference,
                color,
                pct_difference
            )
        )
    get_collateral_amount_contract.short_description = 'Collateral Amount Contract'

    def get_borrow_amount_contract(self, obj):
        provider = AaveDataProvider(obj.network)
        user_reserve = provider.getUserReserveData(obj.asset.asset, [obj.address])[0]['result']
        borrow_amount_contract = Decimal(user_reserve.currentVariableDebt) / obj.asset.decimals
        borrow_amount_live = obj.get_scaled_balance("borrow")

        if borrow_amount_live:
            difference = borrow_amount_live - borrow_amount_contract
        else:
            difference = 0

        if difference and borrow_amount_contract != Decimal('0'):
            pct_difference = (difference / borrow_amount_contract) * Decimal('10000.000000')
        else:
            pct_difference = 0

        # Determine color for the difference
        color = 'green' if difference and difference >= 0 else 'red'
        return mark_safe(
            '''
            <div>
                <div>Contract: <span style="color: blue">{}</span></div>
                <div>Live: <span style="color: blue">{}</span></div>
                <div>Difference: <span style="color: {}">{:+}</span></div>
                <div>% Difference: <span style="color: {}">{:+.6f} bps</span></div>
            </div>
            '''.format(
                borrow_amount_contract,
                borrow_amount_live or 0,
                color,
                difference,
                color,
                pct_difference
            )
        )
    get_borrow_amount_contract.short_description = 'Borrow Amount Contract'

    def get_collateral_aggregate_amounts(self, obj):
        total_mint = (
            obj.aavemintevent_set.filter(type="collateral").aggregate(
                models.Sum('value'))['value__sum'] or Decimal('0')
        )
        total_mint_interest = (
            obj.aavemintevent_set.filter(type="collateral").aggregate(
                models.Sum('balance_increase'))['balance_increase__sum'] or Decimal('0')
        )
        total_burn = (
            obj.aaveburnevent_set.filter(type="collateral").aggregate(
                models.Sum('value'))['value__sum'] or Decimal('0')
        )
        total_burn_interest = (
            obj.aaveburnevent_set.filter(type="collateral").aggregate(
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

        event_diff = total_supply - total_withdraw

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
                <div>Total Event Diff: <span style="color: {};">{:+}</span></div>
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
                total_withdraw,
                'green' if event_diff >= 0 else 'red',
                event_diff
            )
        )
    get_collateral_aggregate_amounts.short_description = 'Aggregate Collateral Event Amounts'

    def get_borrow_aggregate_amounts(self, obj):
        total_mint = (
            obj.aavemintevent_set.filter(type="borrow").aggregate(
                models.Sum('value'))['value__sum'] or Decimal('0')
        )
        total_mint_interest = (
            obj.aavemintevent_set.filter(type="borrow").aggregate(
                models.Sum('balance_increase'))['balance_increase__sum'] or Decimal('0')
        )
        total_burn = (
            obj.aaveburnevent_set.filter(type="borrow").aggregate(
                models.Sum('value'))['value__sum'] or Decimal('0')
        )
        total_burn_interest = (
            obj.aaveburnevent_set.filter(type="borrow").aggregate(
                models.Sum('balance_increase'))['balance_increase__sum'] or Decimal('0')
        )
        total_borrow = (
            obj.aaveborrowevent_set.aggregate(
                models.Sum('amount'))['amount__sum'] or Decimal('0')
        )
        total_repay = (
            obj.aaverepayevent_set.aggregate(
                models.Sum('amount'))['amount__sum'] or Decimal('0')
        )

        mint_burn_diff = total_mint - total_burn
        mint_burn_interest_diff = total_mint_interest - total_burn_interest
        event_diff = total_borrow - total_repay

        return mark_safe(
            '''
            <div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
                <div>Total Mint: <span style="color: green">{:+}</span>
                (Interest: <span style="color: green">{:+}</span>)</div>
                <div>Total Burn: <span style="color: red">{:+}</span>
                (Interest: <span style="color: red">{:+}</span>)</div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
                <div>Token Model Balance: <span style="color: {};">{:+}</span>
                (Interest: <span style="color: {};">{:+}</span>)</div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
                <div>Total Borrow: <span style="color: green">{:+}</span></div>
                <div>Total Repay: <span style="color: red">{:+}</span></div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
                <div>Total Event Diff: <span style="color: {};">{:+}</span></div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
            </div>
            '''.format(
                total_mint,
                total_mint_interest,
                total_burn,
                total_burn_interest,
                'green' if mint_burn_diff >= 0 else 'red',
                mint_burn_diff,
                'green' if mint_burn_interest_diff >= 0 else 'red',
                mint_burn_interest_diff,
                total_borrow,
                total_repay,
                'green' if event_diff >= 0 else 'red',
                event_diff
            )
        )
    get_borrow_aggregate_amounts.short_description = 'Aggregate Borrow Event Amounts'

    def get_user_reserve_data(self, obj):
        provider = AaveDataProvider(obj.network.name)
        user_reserve = provider.getUserReserveData(obj.asset.asset, [obj.address])[0]['result']
        return format_pretty_json(dict(user_reserve))
    get_user_reserve_data.short_description = 'User Reserve Data'

    def get_collateral_indexes(self, obj):
        provider = AaveDataProvider(obj.network.name)
        previous_collateral_index = provider.getPreviousIndex(
            obj.asset.atoken_address,
            [obj.address]
        )[0]['result'].index
        db_index = obj.last_updated_collateral_liquidity_index

        return mark_safe(
            '''
            <div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
                <div>Contract Value: <span>{}</span></div>
                <div>Database Value: <span>{}</span></div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
                <div>Diff: <span style="color: {};">{:+}</span></div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
            </div>
            '''.format(
                previous_collateral_index,
                db_index,
                'green' if previous_collateral_index >= db_index else 'red',
                previous_collateral_index - db_index
            )
        )
    get_collateral_indexes.short_description = 'Collateral Indexes'

    def get_borrow_indexes(self, obj):
        provider = AaveDataProvider(obj.network.name)
        previous_borrow_index = provider.getPreviousIndex(
            obj.asset.variable_debt_token_address,
            [obj.address]
        )[0]['result'].index
        db_index = obj.last_updated_borrow_liquidity_index

        return mark_safe(
            '''
            <div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
                <div>Contract Value: <span>{}</span></div>
                <div>Database Value: <span>{}</span></div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
                <div>Diff: <span style="color: {};">{:+}</span></div>
                <div style="border-bottom: 1px solid #ccc; margin: 10px 0;"></div>
            </div>
            '''.format(
                previous_borrow_index,
                db_index,
                'green' if previous_borrow_index >= db_index else 'red',
                previous_borrow_index - db_index
            )
        )
    get_borrow_indexes.short_description = 'Borrow Indexes'


@admin.register(AaveDataQualityAnalyticsReport)
class AaveDataQualityAnalyticsReportAdmin(admin.ModelAdmin):
    list_display = (
        'date',
        'network',
        'get_collateral_verification_rate',
        'get_borrow_verification_rate',
        'num_collateral_verified',
        'num_collateral_unverified',
        'num_collateral_deleted',
        'num_borrow_verified',
        'num_borrow_unverified',
        'num_borrow_deleted',
    )

    list_filter = (
        'network',
        'date',
    )

    readonly_fields = (
        'date',
        'network',
        'num_collateral_verified',
        'num_collateral_unverified',
        'num_collateral_deleted',
        'num_borrow_verified',
        'num_borrow_unverified',
        'num_borrow_deleted',
        'get_collateral_verification_rate',
        'get_borrow_verification_rate',
    )

    fieldsets = (
        ('Report Information', {
            'fields': (
                'date',
                'network',
            )
        }),
        ('Collateral Metrics', {
            'fields': (
                'num_collateral_verified',
                'num_collateral_unverified',
                'num_collateral_deleted',
                'get_collateral_verification_rate',
            )
        }),
        ('Borrow Metrics', {
            'fields': (
                'num_borrow_verified',
                'num_borrow_unverified',
                'num_borrow_deleted',
                'get_borrow_verification_rate',
            )
        }),
    )

    ordering = ('-date',)

    def get_collateral_verification_rate(self, obj):
        total = obj.num_collateral_verified + obj.num_collateral_unverified
        if total == 0:
            return '0%'
        rate = (obj.num_collateral_verified / total) * 100
        return f'{rate:.2f}%'
    get_collateral_verification_rate.short_description = 'Collateral Verification Rate'

    def get_borrow_verification_rate(self, obj):
        total = obj.num_borrow_verified + obj.num_borrow_unverified
        if total == 0:
            return '0%'
        rate = (obj.num_borrow_verified / total) * 100
        return f'{rate:.2f}%'
    get_borrow_verification_rate.short_description = 'Borrow Verification Rate'

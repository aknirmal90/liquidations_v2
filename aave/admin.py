from django.contrib import admin

from aave.models import Asset
from utils.admin import EnableDisableAdminMixin, get_explorer_address_url


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
                ('get_contractA_link', 'priceA', 'numerator', 'updated_at_block_heightA'),
                ('get_contractB_link', 'priceB', 'denominator', 'updated_at_block_heightB')
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
        'numerator',
        'denominator',
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

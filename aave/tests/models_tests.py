from decimal import Decimal

import pytest
from django.utils import timezone

from aave.models import (
    AaveBalanceLog,
    AaveBorrowEvent,
    AaveBurnEvent,
    AaveDataQualityAnalyticsReport,
    AaveLiquidationCallEvent,
    AaveLiquidationLog,
    AaveMintEvent,
    AaveRepayEvent,
    AaveSupplyEvent,
    AaveTransferEvent,
    AaveUser,
    AaveWithdrawEvent,
    Asset,
    AssetPriceLog,
)
from blockchains.models import Network


@pytest.mark.django_db
def test_asset_default_values():
    """Test that default values are set properly on Asset model."""
    network = Network.objects.create(name="DefaultTestNet", chain_id="1234")
    asset = Asset.objects.create(
        asset="0xSomeAddress",
        network=network,
        symbol="SYM"
    )
    assert asset.is_enabled is False
    assert asset.liquidation_threshold == Decimal("0")
    assert asset.liquidation_bonus == Decimal("0")
    assert asset.emode_liquidation_threshold is None
    assert asset.emode_liquidation_bonus is None
    assert asset.price_type is None
    assert asset.priceA is None
    assert asset.priceB is None
    assert asset.decimals_price is None
    assert asset.max_cap is None
    assert asset.price is None
    assert asset.price_in_nativeasset is None
    assert asset.emode_category == 0
    assert asset.collateral_liquidity_index == Decimal("0.0")
    assert asset.borrow_liquidity_index == Decimal("0.0")
    assert asset.num_decimals == Decimal("0")
    assert asset.decimals == Decimal("0")


@pytest.mark.django_db
def test_asset_price_log_default_values():
    """Test that default values are set properly on AssetPriceLog model."""
    network = Network.objects.create(name="DefaultTestNet2", chain_id="5678")
    apl = AssetPriceLog.objects.create(
        aggregator_address="0xAggregator",
        network=network,
        provider="SomeProvider"
    )
    assert apl.transaction_hash is None
    assert apl.price is None
    assert apl.onchain_created_at is None
    assert apl.onchain_received_at is None
    assert apl.processed_at is None
    assert apl.round_id is None
    # db_created_at uses auto_now_add, so just check it's not None
    assert apl.db_created_at is not None


@pytest.mark.django_db
def test_aave_liquidation_log_default_values():
    """Test that default values are set properly on AaveLiquidationLog model."""
    network = Network.objects.create(name="DefaultTestNet3", chain_id="9012")
    liquidation_log = AaveLiquidationLog.objects.create(
        network=network,
        user="0xUser",
        liquidator="0xLiquidator"
    )
    assert liquidation_log.debt_to_cover is None
    assert liquidation_log.debt_to_cover_in_usd is None
    assert liquidation_log.liquidated_collateral_amount is None
    assert liquidation_log.liquidated_collateral_amount_in_usd is None
    assert liquidation_log.profit_in_usd is None
    assert liquidation_log.collateral_asset is None
    assert liquidation_log.debt_asset is None
    assert liquidation_log.block_datetime is None
    assert liquidation_log.block_height is None
    assert liquidation_log.transaction_hash is None
    assert liquidation_log.transaction_index is None
    assert liquidation_log.onchain_created_at is None
    assert liquidation_log.onchain_received_at is None
    # db_created_at uses auto_now_add
    assert liquidation_log.db_created_at is not None
    assert liquidation_log.health_factor_before_tx is None
    assert liquidation_log.health_factor_before_zero_blocks is None
    assert liquidation_log.health_factor_before_one_blocks is None
    assert liquidation_log.health_factor_before_two_blocks is None
    assert liquidation_log.health_factor_before_three_blocks is None


@pytest.mark.django_db
def test_aave_balance_log_default_values():
    """Test that default values are set properly on AaveBalanceLog model."""
    network = Network.objects.create(name="DefaultTestNet4", chain_id="9999")
    asset = Asset.objects.create(
        asset="0xAnotherAddress",
        network=network,
        symbol="TEST"
    )
    balance_log = AaveBalanceLog.objects.create(
        network=network,
        address="0xUserAddress",
        asset=asset
    )
    assert balance_log.price_in_nativeasset is None
    assert balance_log.last_updated_collateral_liquidity_index == Decimal("0.0")
    assert balance_log.last_updated_borrow_liquidity_index == Decimal("0.0")
    assert balance_log.collateral_amount == Decimal("0.0")
    assert balance_log.collateral_amount_live == Decimal("0.0")
    assert balance_log.collateral_amount_live_with_liquidation_threshold == Decimal("0.0")
    assert balance_log.collateral_amount_live_is_verified is None
    assert balance_log.collateral_is_enabled is False
    assert balance_log.collateral_is_enabled_updated_at_block == 0
    assert balance_log.collateral_health_factor == Decimal("0.0")
    assert balance_log.borrow_amount == Decimal("0.0")
    assert balance_log.borrow_amount_live == Decimal("0.0")
    assert balance_log.borrow_amount_live_is_verified is None
    assert balance_log.borrow_is_enabled is False
    assert balance_log.mark_for_deletion is False
    assert balance_log.emode_category == 0
    assert balance_log.emode_category_updated_at_block == 0
    assert balance_log.user is None


@pytest.mark.django_db
def test_aave_transfer_event_default_values():
    """Test that default values are set properly on AaveTransferEvent model."""
    network = Network.objects.create(name="DefaultTestNet5", chain_id="1010")
    asset = Asset.objects.create(
        asset="0xABCD",
        network=network,
        symbol="TRF"
    )
    balance_log = AaveBalanceLog.objects.create(
        network=network,
        address="0xUser",
        asset=asset
    )
    aave_transfer_event = AaveTransferEvent.objects.create(
        balance_log=balance_log,
        from_address="0xFrom",
        to_address="0xTo",
        value=Decimal("100"),
        block_height=1234,
        transaction_hash="0xTRX",
        transaction_index=1,
        log_index=0
    )
    # Check that no unexpected defaults exist; these fields are all required at creation
    assert aave_transfer_event.value == Decimal("100")


@pytest.mark.django_db
def test_aave_mint_event_default_values():
    """Test that default values are set properly on AaveMintEvent model."""
    network = Network.objects.create(name="DefaultTestNet6", chain_id="1111")
    asset = Asset.objects.create(
        asset="0xMINT",
        network=network,
        symbol="MNT"
    )
    balance_log = AaveBalanceLog.objects.create(
        network=network,
        address="0xMintUser",
        asset=asset
    )
    mint_event = AaveMintEvent.objects.create(
        balance_log=balance_log,
        caller="0xCaller",
        on_behalf_of="0xOBOf",
        value=Decimal("100"),
        balance_increase=Decimal("10"),
        index=Decimal("1"),
        block_height=10,
        transaction_hash="0xMINTTRX",
        transaction_index=1,
        log_index=0
    )
    # 'type' defaults to 'collateral'
    assert mint_event.type == "collateral"


@pytest.mark.django_db
def test_aave_burn_event_default_values():
    """Test that default values are set properly on AaveBurnEvent model."""
    network = Network.objects.create(name="DefaultTestNet7", chain_id="2222")
    asset = Asset.objects.create(asset="0xBURN", network=network, symbol="BRN")
    balance_log = AaveBalanceLog.objects.create(
        network=network,
        address="0xBurnUser",
        asset=asset
    )
    burn_event = AaveBurnEvent.objects.create(
        balance_log=balance_log,
        from_address="0xFromBurn",
        target="0xTarget",
        value=Decimal("100"),
        balance_increase=Decimal("5"),
        index=Decimal("1"),
        block_height=20,
        transaction_hash="0xBURNTRX",
        transaction_index=1,
        log_index=0
    )
    # 'type' defaults to 'collateral'
    assert burn_event.type == "collateral"


@pytest.mark.django_db
def test_aave_supply_event_default_values():
    """Test that default values are set properly on AaveSupplyEvent model."""
    network = Network.objects.create(name="DefaultTestNet8", chain_id="3333")
    asset = Asset.objects.create(
        asset="0xSUPPLY",
        network=network,
        symbol="SUP"
    )
    balance_log = AaveBalanceLog.objects.create(
        network=network,
        address="0xSupplyUser",
        asset=asset
    )
    supply_event = AaveSupplyEvent.objects.create(
        balance_log=balance_log,
        user="0xUsr",
        on_behalf_of="0xOBOf",
        amount=Decimal("100"),
        referral_code=42,
        block_height=123,
        transaction_hash="0xSUPPLYTRX",
        transaction_index=1,
        log_index=0
    )
    # These fields are all required; no default to test. Just ensure creation works.
    assert supply_event.amount == Decimal("100")


@pytest.mark.django_db
def test_aave_withdraw_event_default_values():
    """Test that default values are set properly on AaveWithdrawEvent model."""
    network = Network.objects.create(name="DefaultTestNet9", chain_id="4444")
    asset = Asset.objects.create(
        asset="0xWITHDRAW",
        network=network,
        symbol="WDW"
    )
    balance_log = AaveBalanceLog.objects.create(
        network=network,
        address="0xWithdrawUser",
        asset=asset
    )
    withdraw_event = AaveWithdrawEvent.objects.create(
        balance_log=balance_log,
        user="0xUsrWD",
        to_address="0xToWD",
        amount=Decimal("100"),
        block_height=456,
        transaction_hash="0xWITHDRAWTRX",
        transaction_index=1,
        log_index=0
    )
    assert withdraw_event.amount == Decimal("100")


@pytest.mark.django_db
def test_aave_data_quality_analytics_report_default_values():
    """Test that default values are set properly on AaveDataQualityAnalyticsReport model."""
    network = Network.objects.create(name="DefaultTestNet10", chain_id="5555")
    report = AaveDataQualityAnalyticsReport.objects.create(
        network=network,
        date=timezone.now().date()
    )
    assert report.num_collateral_verified == 0
    assert report.num_borrow_verified == 0
    assert report.num_collateral_unverified == 0
    assert report.num_borrow_unverified == 0
    assert report.num_collateral_deleted == 0
    assert report.num_borrow_deleted == 0


@pytest.mark.django_db
def test_aave_borrow_event_default_values():
    """Test that default values are set properly on AaveBorrowEvent model."""
    network = Network.objects.create(name="DefaultTestNet11", chain_id="6666")
    asset = Asset.objects.create(
        asset="0xBORROW",
        network=network,
        symbol="BRW"
    )
    balance_log = AaveBalanceLog.objects.create(
        network=network,
        address="0xBorrowUser",
        asset=asset
    )
    borrow_event = AaveBorrowEvent.objects.create(
        balance_log=balance_log,
        user="0xUsrBRW",
        on_behalf_of="0xOBOfBRW",
        amount=Decimal("100"),
        interest_rate_mode=2,
        borrow_rate=Decimal("0.1"),
        referral_code=777,
        block_height=89,
        transaction_hash="0xBORROWTRX",
        transaction_index=1,
        log_index=0
    )
    assert borrow_event.amount == Decimal("100")


@pytest.mark.django_db
def test_aave_repay_event_default_values():
    """Test that default values are set properly on AaveRepayEvent model."""
    network = Network.objects.create(name="DefaultTestNet12", chain_id="7777")
    asset = Asset.objects.create(
        asset="0xREPAY",
        network=network,
        symbol="RPY"
    )
    balance_log = AaveBalanceLog.objects.create(
        network=network,
        address="0xRepayUser",
        asset=asset
    )
    repay_event = AaveRepayEvent.objects.create(
        balance_log=balance_log,
        user="0xUsrRPY",
        repayer="0xRepRPY",
        amount=Decimal("50"),
        use_a_tokens=True,
        block_height=999,
        transaction_hash="0xREPAYTRX",
        transaction_index=1,
        log_index=0
    )
    assert repay_event.amount == Decimal("50")


@pytest.mark.django_db
def test_aave_liquidation_call_event_default_values():
    """Test that default values are set properly on AaveLiquidationCallEvent model."""
    network = Network.objects.create(name="DefaultTestNet13", chain_id="8888")
    asset = Asset.objects.create(
        asset="0xLIQD",
        network=network,
        symbol="LQ"
    )
    balance_log = AaveBalanceLog.objects.create(
        network=network,
        address="0xLiquidationUser",
        asset=asset
    )
    liquidation_call_event = AaveLiquidationCallEvent.objects.create(
        balance_log=balance_log,
        collateral_asset="0xColAsset",
        debt_asset="0xDebtAsset",
        user="0xUsrLQD",
        debt_to_cover=Decimal("200"),
        liquidated_collateral_amount=Decimal("100"),
        liquidator="0xLiquidator",
        receive_a_token=False,
        block_height=1234,
        transaction_hash="0xLIQDTRX",
        transaction_index=1,
        log_index=0
    )
    assert liquidation_call_event.debt_to_cover == Decimal("200")
    assert liquidation_call_event.liquidated_collateral_amount == Decimal("100")


@pytest.mark.django_db
def test_aave_user_default_values():
    """Test that default values are set properly on AaveUser model."""
    network = Network.objects.create(name="DefaultTestNet14", chain_id="9998")
    user = AaveUser.objects.create(
        network=network,
        address="0xUserAddress"
    )
    assert user.health_factor is None

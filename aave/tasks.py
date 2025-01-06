import logging
import math
from datetime import datetime, timezone
from decimal import Decimal

from celery import Task
from django.core.cache import cache
from web3 import Web3

from aave.dataprovider import AaveDataProvider
from aave.models import (
    AaveBalanceLog,
    AaveBurnEvent,
    AaveLiquidationLog,
    AaveMintEvent,
    AaveSupplyEvent,
    AaveTransferEvent,
    AaveWithdrawEvent,
    Asset,
    AssetPriceLog,
)
from blockchains.models import Network
from liquidations_v2.celery_app import app
from utils.constants import BALANCES_AMOUNT_ERROR_THRESHOLD
from utils.simulation import get_simulated_health_factor
from utils.tokens import EvmTokenRetriever

logger = logging.getLogger(__name__)


class ResetAssetsTask(Task):
    """Task to reset all Asset model instances and AssetPriceLog instances."""

    def run(self):
        """Delete all Asset and AssetPriceLog model instances."""
        logger.info("Starting ResetAssetsTask")

        liquidation_count = AaveLiquidationLog.objects.count()
        AaveLiquidationLog.objects.all().delete()
        logger.info(f"Successfully deleted {liquidation_count} AaveLiquidationLog instances")

        burn_count = AaveBurnEvent.objects.count()
        AaveBurnEvent.objects.all().delete()
        logger.info(f"Successfully deleted {burn_count} AaveBurnEvent instances")

        mint_count = AaveMintEvent.objects.count()
        AaveMintEvent.objects.all().delete()
        logger.info(f"Successfully deleted {mint_count} AaveMintEvent instances")

        transfer_count = AaveTransferEvent.objects.count()
        AaveTransferEvent.objects.all().delete()
        logger.info(f"Successfully deleted {transfer_count} AaveTransferEvent instances")

        supply_count = AaveSupplyEvent.objects.count()
        AaveSupplyEvent.objects.all().delete()
        logger.info(f"Successfully deleted {supply_count} AaveSupplyEvent instances")

        withdraw_count = AaveWithdrawEvent.objects.count()
        AaveWithdrawEvent.objects.all().delete()
        logger.info(f"Successfully deleted {withdraw_count} AaveWithdrawEvent instances")

        balance_count = AaveBalanceLog.objects.count()
        logger.info(f"Found {balance_count} AaveBalanceLog instances to delete")

        # Delete in batches of 10000 using min/max PKs for efficiency
        batch_size = 10000
        min_pk = AaveBalanceLog.objects.order_by('pk').first().pk
        max_pk = AaveBalanceLog.objects.order_by('-pk').first().pk

        for batch_start in range(min_pk, max_pk + 1, batch_size):
            batch_end = min(batch_start + batch_size, max_pk + 1)
            deleted_count = AaveBalanceLog.objects.filter(
                pk__gte=batch_start,
                pk__lt=batch_end
            ).delete()[0]
            logger.info(
                f"Deleted batch of {deleted_count} AaveBalanceLog instances (PKs {batch_start} to {batch_end-1})"
            )

        logger.info(f"Successfully deleted all {balance_count} AaveBalanceLog instances")

        # price_log_count = AssetPriceLog.objects.count()
        # AssetPriceLog.objects.all().delete()
        # logger.info(f"Successfully deleted {price_log_count} AssetPriceLog instances")

        # asset_count = Asset.objects.count()
        # Asset.objects.all().delete()
        # logger.info(f"Successfully deleted {asset_count} Aave Asset instances")

        logger.info("Completed ResetAssetsTask")


ResetAssetsTask = app.register_task(ResetAssetsTask())


class UpdateAssetMetadataTask(Task):
    """Task to update token metadata for assets with missing symbols."""

    acks_late = False

    def run(self):
        """Update metadata for assets with null/blank symbols."""
        logger.info("Starting UpdateAssetMetadataTask")

        assets = Asset.objects.filter(symbol__isnull=True) | Asset.objects.filter(symbol='')
        count = assets.count()

        if count == 0:
            logger.info("No assets found requiring metadata update")
            return

        logger.info(f"Found {count} assets requiring metadata update")

        for asset in assets:
            try:
                token_retriever = EvmTokenRetriever(
                    network_name=asset.network.name,
                    token_address=asset.asset
                )
                asset.symbol = token_retriever.name
                asset.num_decimals = token_retriever.num_decimals
                asset.decimals = math.pow(10, token_retriever.num_decimals)
                asset.save(update_fields=['symbol', 'num_decimals', 'decimals'])
                logger.info(f"Updated metadata for asset {asset.asset}")
            except Exception as e:
                logger.error(f"Failed to update metadata for asset {asset.asset}: {str(e)}")

        logger.info("Completed UpdateAssetMetadataTask")


UpdateAssetMetadataTask = app.register_task(UpdateAssetMetadataTask())


class UpdateAssetPriceTask(Task):
    """Task to update asset prices and create price logs."""

    acks_late = False

    def get_global_cache_key(self, network_name, contract):
        return f"price-{network_name}-{contract}"

    def is_price_updated(self, network_name, contract, new_price):
        global_cache_key = self.get_global_cache_key(network_name, contract)
        cached_price = cache.get(global_cache_key)
        return cached_price == new_price

    def run(
        self,
        network_id,
        network_name,
        contract,
        new_price,
        provider,
        onchain_received_at,
        transaction_hash,
        processed_at,
        onchain_created_at=None,
        round_id=None
    ):
        assets_to_update = []
        is_price_updated = self.is_price_updated(network_name, contract, new_price)

        if is_price_updated:
            # Update assets where contract matches contractA
            assetsA = Asset.objects.filter(contractA__iexact=contract)
            assetsA.update(
                priceA=Decimal(new_price),
            )
            for asset in assetsA:
                try:
                    price, price_in_usdt = asset.get_price()
                    asset.price = price
                    asset.price_in_usdt = price_in_usdt
                    assets_to_update.append(asset)
                except Exception as e:
                    logger.error(f"Failed to get price for asset {asset.asset}: {str(e)}")
                    continue

            # Update assets where contract matches contractB
            assetsB = Asset.objects.filter(contractB__iexact=contract)
            assetsB.update(
                priceB=Decimal(new_price),
            )
            for asset in assetsB:
                try:
                    price, price_in_usdt = asset.get_price()
                    asset.price = price
                    asset.price_in_usdt = price_in_usdt
                    assets_to_update.append(asset)
                except Exception as e:
                    logger.error(f"Failed to get price for asset {asset.asset}: {str(e)}")
                    continue

            # Bulk save all updated assets
            Asset.objects.bulk_update(assets_to_update, ['price', 'price_in_usdt'])

        if onchain_created_at:
            onchain_created_at = datetime.fromtimestamp(onchain_created_at, tz=timezone.utc)

        AssetPriceLog.objects.create(
            aggregator_address=contract,
            network_id=network_id,
            price=new_price,
            onchain_created_at=onchain_created_at,
            round_id=round_id,
            onchain_received_at=onchain_received_at,
            provider=provider,
            transaction_hash=transaction_hash,
            processed_at=processed_at
        )
        if not provider.startswith("sequencer"):
            AssetPriceLog.objects.filter(
                transaction_hash=transaction_hash,
                network_id=network_id
            ).update(
                onchain_created_at=onchain_created_at,
                round_id=round_id
            )


UpdateAssetPriceTask = app.register_task(UpdateAssetPriceTask())


class UpdateMaxCappedRatiosTask(Task):
    """Task to update max capped ratios for assets."""

    def run(self):
        """Update max capped ratios for assets."""
        assets = Asset.objects.filter(price_type=Asset.PriceType.RATIO)
        functions = [
            ("MINIMUM_SNAPSHOT_DELAY", "uint48"),
            ("getMaxRatioGrowthPerSecond", "uint256"),
            ("getSnapshotRatio", "uint256"),
            ("getSnapshotTimestamp", "uint256")
        ]
        abi = []
        for func_name, return_type in functions:
            abi.append({
                "inputs": [],
                "name": func_name,
                "outputs": [{"type": return_type}],
                "stateMutability": "view",
                "type": "function"
            })
        for asset in assets:
            try:
                w3 = Web3(Web3.HTTPProvider(asset.network.rpc))
                contract = w3.eth.contract(address=Web3.to_checksum_address(asset.pricesource), abi=abi)

                results = {}
                for func_name, _ in functions:
                    try:
                        result = contract.functions[func_name]().call()
                        results[func_name] = result
                    except Exception as e:
                        logger.error(f"Failed to call {func_name} for asset {asset.asset}: {str(e)}")
                        continue

                current_ts = int(datetime.now(timezone.utc).timestamp())
                current_delay = current_ts - results['getSnapshotTimestamp']
                if current_delay < results['MINIMUM_SNAPSHOT_DELAY']:
                    continue

                max_cap = results['getSnapshotRatio'] + results['getMaxRatioGrowthPerSecond'] * current_delay
                asset.max_cap = max_cap
                asset.save(update_fields=['max_cap'])

                logger.info(f"Got results for asset {asset.asset}: {results}")

            except Exception as e:
                logger.error(f"Failed to process asset {asset.asset}: {str(e)}")
                continue


UpdateMaxCappedRatiosTask = app.register_task(UpdateMaxCappedRatiosTask())


class UpdateSimulatedHealthFactorTask(Task):
    """Task to update simulated health factor for aave liquidation logs."""

    def run(self):
        """Update simulated health factor for aave liquidation logs."""
        liquidation_logs = (
            AaveLiquidationLog.objects
            .filter(health_factor_before_tx__isnull=True)
            .order_by('-block_height')[:200]
        )

        total_logs = len(liquidation_logs)
        logger.info(f"Processing {total_logs} liquidation logs")

        for idx, liquidation_log in enumerate(liquidation_logs, 1):

            params = [
                # before tx
                (liquidation_log.block_height,
                 liquidation_log.transaction_index - 1,
                 "health_factor_before_tx"),
                # before tx and zero blocks
                (liquidation_log.block_height,
                 1,
                 "health_factor_before_zero_blocks"),
                # before tx and one block
                (liquidation_log.block_height - 1,
                 1,
                 "health_factor_before_one_blocks"),
                # before tx and two blocks
                (liquidation_log.block_height - 2,
                 1,
                 "health_factor_before_two_blocks"),
                # before tx and three blocks
                (liquidation_log.block_height - 3,
                 1,
                 "health_factor_before_three_blocks"),
            ]

            logger.info(
                f"Processing liquidation log {liquidation_log.id} for user {liquidation_log.user} ({idx}/{total_logs})"
            )
            try:
                for param in params:
                    logger.debug(f"Getting health factor at block {param[0]} tx index {param[1]}")
                    health_factor = get_simulated_health_factor(
                        chain_id=liquidation_log.network.chain_id,
                        address=liquidation_log.user,
                        block_number=param[0],
                        transaction_index=param[1]
                    )
                    logger.info(f"Got health factor {health_factor} for {param[2]}")
                    setattr(liquidation_log, param[2], health_factor)
                liquidation_log.save()
                logger.info(f"Successfully updated health factors for liquidation log {liquidation_log.id}")
            except Exception as e:
                logger.error(
                    f"Failed to process liquidation log {liquidation_log.id}: {str(e)}",
                    exc_info=True
                )
                continue

        logger.info(f"Completed processing {total_logs} liquidation logs")


UpdateSimulatedHealthFactorTask = app.register_task(UpdateSimulatedHealthFactorTask())


class UpdateCollateralAmountLiveIsVerifiedTask(Task):
    """Task to update collateral amount live for aave balance logs."""
    expires = 60 * 60 * 3

    def is_collateral_amount_verified(self, collateral_amount_live, collateral_amount_contract):
        collateral_amount_live = collateral_amount_live.quantize(Decimal('1.00'))
        if abs(collateral_amount_live - collateral_amount_contract) <= Decimal('100.00'):
            return True
        elif collateral_amount_contract != Decimal('0'):
            pct_difference = (collateral_amount_live - collateral_amount_contract) / collateral_amount_contract
            return pct_difference < BALANCES_AMOUNT_ERROR_THRESHOLD
        return False

    def run(self):
        """Update collateral amount live for aave balance logs."""
        networks = Network.objects.all().values_list('name', flat=True)
        for network in networks:
            self._process_network(network)

    def _process_network(self, network_name):
        """Process a single network's assets and balance logs."""
        assets = Asset.objects.filter(network__name=network_name)
        provider = AaveDataProvider(network_name)

        for asset in assets:
            self._process_asset(asset, provider)

    def _process_asset(self, asset, provider):
        """Process balance logs for a single asset."""
        balances = AaveBalanceLog.objects.filter(asset=asset)
        for i in range(0, len(balances), 100):
            batch = balances[i:i + 100]
            self._process_balance_batch(batch, asset, provider)

    def _process_balance_batch(self, batch, asset, provider):
        """Process a batch of balance logs."""
        logger.info(f"Processing batch of {len(batch)} balance logs for asset {asset.symbol}")

        user_reserves = provider.getUserReserveData(
            asset.asset,
            [obj.address for obj in batch]
        )
        logger.info(f"Retrieved user reserve data from provider for {len(user_reserves)} users")

        updated_batch = self._update_batch_verification(
            batch=batch,
            user_reserves=user_reserves,
            asset=asset
        )
        logger.info("Updated batch verification status")

        # Bulk update the batch
        AaveBalanceLog.objects.bulk_update(
            updated_batch,
            ['collateral_amount_live_is_verified', 'collateral_amount_live', 'mark_for_deletion']
        )
        logger.info(f"Successfully updated verification status for {len(batch)} balance logs")

    def _update_batch_verification(self, batch, user_reserves, asset):
        """Update verification status for a batch of balance logs."""
        updated_batch = []
        for i, contract_user_reserve in enumerate(user_reserves):
            db_user_reserve = batch[i]
            collateral_amount_contract = Decimal(contract_user_reserve["result"].currentATokenBalance)

            if db_user_reserve.last_updated_liquidity_index:
                collateral_amount_live = (
                    db_user_reserve.collateral_amount * (
                        asset.liquidity_index / db_user_reserve.last_updated_liquidity_index
                    )
                )
            else:
                collateral_amount_live = db_user_reserve.collateral_amount

            db_user_reserve.collateral_amount_live = collateral_amount_live
            db_user_reserve.collateral_amount_live_is_verified = self.is_collateral_amount_verified(
                collateral_amount_live,
                collateral_amount_contract
            )
            if collateral_amount_contract == Decimal('0'):
                db_user_reserve.mark_for_deletion = True
            updated_batch.append(db_user_reserve)
        return updated_batch


UpdateCollateralAmountLiveIsVerifiedTask = app.register_task(UpdateCollateralAmountLiveIsVerifiedTask())


class VerifyReserveConfigurationTask(Task):
    """Task to verify reserve configuration for assets."""

    def run(self):
        """Verify reserve configuration for assets."""
        networks = Network.objects.all()
        for network in networks:
            assets = Asset.objects.filter(network=network)
            assets_addresses = [asset.asset for asset in assets]
            dataprovider = AaveDataProvider(network.name)

            reserve_configuration_data = dataprovider.getReserveConfigurationData(assets_addresses)
            reserve_tokens_addresses = dataprovider.getReserveTokensAddresses(assets_addresses)
            price_sources = dataprovider.getSourceOfAsset(assets_addresses)
            logger.info(f"Got reserve configuration data for {len(reserve_configuration_data)} assets")

            for i in range(len(assets)):
                self.validate_asset(
                    asset=assets[i],
                    reserve_configuration_data=reserve_configuration_data[i],
                    reserve_tokens_addresses=reserve_tokens_addresses[i],
                    price_sources=price_sources[i]
                )

    def validate_asset(self, asset, reserve_configuration_data, reserve_tokens_addresses, price_sources):
        """Validate asset configuration."""
        reserve_configuration_fields = (
            ('decimals', 'num_decimals'),
            ('liquidationThreshold', 'liquidation_threshold'),
            ('liquidationBonus', 'liquidation_bonus'),
            ('reserveFactor', 'reserve_factor'),
        )

        reserve_tokens_fields = (
            ("aTokenAddress", "atoken_address"),
            ("stableDebtTokenAddress", "stable_debt_token_address"),
            ("variableDebtTokenAddress", "variable_debt_token_address"),
        )

        price_source_fields = (
            ("source", "pricesource"),
        )

        has_mismatches = False

        for attr, field in reserve_configuration_fields:
            contract_value = reserve_configuration_data['result'][attr]
            db_value = getattr(asset, field)

            if isinstance(db_value, Decimal):
                db_value = int(db_value)

            if contract_value != db_value:
                has_mismatches = True
                logger.error(f"Mismatch for {attr} on {asset.symbol} - Contract: {contract_value}, DB: {db_value}")

        for attr, field in reserve_tokens_fields:
            contract_value = reserve_tokens_addresses['result'][attr]
            db_value = getattr(asset, field)
            if contract_value != db_value:
                has_mismatches = True
                logger.error(f"Mismatch for {attr} on {asset.symbol} - Contract: {contract_value}, DB: {db_value}")

        for attr, field in price_source_fields:
            contract_value = price_sources['result'][attr]
            db_value = getattr(asset, field)
            if contract_value != db_value:
                has_mismatches = True
                logger.error(f"Mismatch for {attr} on {asset.symbol} - Contract: {contract_value}, DB: {db_value}")

        if not has_mismatches:
            logger.info(f"All configurations matched for {asset.symbol}")


VerifyReserveConfigurationTask = app.register_task(VerifyReserveConfigurationTask())

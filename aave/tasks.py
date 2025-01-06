import logging
import math
from datetime import datetime, timezone
from decimal import Decimal, DivisionByZero

from celery import Task
from django.core.cache import cache
from django.db import models
from web3 import Web3

from aave.dataprovider import AaveDataProvider
from aave.models import AaveBalanceLog, AaveDataQualityAnalyticsReport, AaveLiquidationLog, Asset, AssetPriceLog
from blockchains.models import Event, Network, Protocol
from liquidations_v2.celery_app import app
from utils.constants import BALANCES_AMOUNT_ERROR_THRESHOLD_PCT, BALANCES_AMOUNT_ERROR_THRESHOLD_VALUE
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

        balance_count = AaveBalanceLog.objects.count()
        logger.info(f"Found {balance_count} AaveBalanceLog instances to delete")

        Event.objects.filter(protocol__name="aave").update(last_synced_block=0)

        # Delete in batches of 10000 using min/max PKs for efficiency
        batch_size = 10000
        first_record = AaveBalanceLog.objects.order_by('pk').first()
        last_record = AaveBalanceLog.objects.order_by('-pk').first()

        if not first_record or not last_record:
            logger.info("No AaveBalanceLog records found to delete")
            return

        min_pk = first_record.pk
        max_pk = last_record.pk

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


class VerifyBalancesTask(Task):
    """Task to update collateral and borrow amounts live for aave balance logs."""
    expires = 60 * 60 * 3

    def run(self):
        """Update collateral and borrow amounts live for aave balance logs."""
        protocol = Protocol.objects.get(name="aave")

        # Process networks
        networks = Network.objects.all()
        for network in networks:
            self._process_network(network, protocol)
            self._generate_analytics_report(network, protocol)
            self._delete_marked_records(network, protocol)

    def is_collateral_amount_verified(self, collateral_amount_live, collateral_amount_contract):
        if abs(collateral_amount_live - collateral_amount_contract) <= BALANCES_AMOUNT_ERROR_THRESHOLD_VALUE:
            return True
        else:
            try:
                pct_difference = (collateral_amount_live - collateral_amount_contract) / collateral_amount_contract
                return pct_difference < BALANCES_AMOUNT_ERROR_THRESHOLD_PCT
            except DivisionByZero:
                return False

    def is_borrow_amount_verified(self, borrow_amount_live, borrow_amount_contract):
        if abs(borrow_amount_live - borrow_amount_contract) <= BALANCES_AMOUNT_ERROR_THRESHOLD_VALUE:
            return True
        else:
            try:
                pct_difference = (borrow_amount_live - borrow_amount_contract) / borrow_amount_contract
                return pct_difference < BALANCES_AMOUNT_ERROR_THRESHOLD_PCT
            except DivisionByZero:
                return False

    def _delete_marked_records(self, network, protocol):
        """Delete records marked for deletion in batches."""
        marked_records = AaveBalanceLog.objects.filter(mark_for_deletion=True, network=network, protocol=protocol)
        if not marked_records.exists():
            return

        min_id = marked_records.order_by('id').first().id
        max_id = marked_records.order_by('-id').first().id

        batch_size = 10000
        for start_id in range(min_id, max_id + 1, batch_size):
            end_id = start_id + batch_size
            batch_to_delete = AaveBalanceLog.objects.filter(
                mark_for_deletion=True,
                network=network,
                protocol=protocol,
                id__gte=start_id,
                id__lt=end_id
            )
            deleted_count = batch_to_delete.delete()[0]
            if deleted_count > 0:
                logger.info(f"Deleted batch of {deleted_count} marked records for {network.name}")

    def _generate_analytics_report(self, network, protocol):
        """Generate analytics report for today's data."""
        today = datetime.now(timezone.utc).date()

        # Get all network/protocol combinations that have balance logs
        balance_logs = AaveBalanceLog.objects.filter(network=network, protocol=protocol)

        # Get metrics for this network/protocol combination
        metrics = balance_logs.aggregate(
            collateral_verified=models.Count(
                'id',
                filter=models.Q(
                    collateral_amount_live_is_verified=True,
                    mark_for_deletion=False
                )
            ),
            collateral_unverified=models.Count(
                'id',
                filter=models.Q(
                    collateral_amount_live_is_verified=False,
                    mark_for_deletion=False
                )
            ),
            borrow_verified=models.Count(
                'id',
                filter=models.Q(
                    borrow_amount_live_is_verified=True,
                    mark_for_deletion=False
                )
            ),
            borrow_unverified=models.Count(
                'id',
                filter=models.Q(
                    borrow_amount_live_is_verified=False,
                    mark_for_deletion=False
                )
            ),
            deleted=models.Count(
                'id',
                filter=models.Q(mark_for_deletion=True)
            )
        )

        # Create or update the report
        report = AaveDataQualityAnalyticsReport.objects.create(
            network=network,
            protocol=protocol,
            date=today,
            num_collateral_verified=metrics['collateral_verified'],
            num_collateral_unverified=metrics['collateral_unverified'],
            num_borrow_verified=metrics['borrow_verified'],
            num_borrow_unverified=metrics['borrow_unverified'],
            num_collateral_deleted=metrics['deleted'],
            num_borrow_deleted=metrics['deleted']
        )

        logger.info(
            f"Generated analytics report for {report.network.name} - "
            f"{report.protocol.name} on {report.date}"
        )

    def _process_network(self, network, protocol):
        """Process a single network's assets and balance logs."""
        assets = Asset.objects.filter(network=network, protocol=protocol)
        provider = AaveDataProvider(network.name)

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
        )
        logger.info("Updated batch verification status")

        # Bulk update the batch
        AaveBalanceLog.objects.bulk_update(
            updated_batch,
            [
                'collateral_amount_live_is_verified',
                'collateral_amount_live',
                'borrow_amount_live_is_verified',
                'borrow_amount_live',
                'mark_for_deletion',
                'collateral_amount',
                'borrow_amount'
            ]
        )
        logger.info(f"Successfully updated verification status for {len(batch)} balance logs")

    def _update_batch_verification(self, batch, user_reserves):
        """Update verification status for a batch of balance logs."""
        updated_batch = []
        for i, contract_user_reserve in enumerate(user_reserves):
            db_user_reserve = batch[i]
            collateral_amount_contract = Decimal(contract_user_reserve["result"].currentATokenBalance)
            borrow_amount_contract = Decimal(contract_user_reserve["result"].currentVariableDebt)

            collateral_amount_live = db_user_reserve.get_scaled_balance(type="collateral")
            borrow_amount_live = db_user_reserve.get_scaled_balance(type="borrow")

            db_user_reserve.collateral_amount_live = collateral_amount_live
            db_user_reserve.borrow_amount_live = borrow_amount_live

            db_user_reserve.collateral_amount_live_is_verified = self.is_collateral_amount_verified(
                db_user_reserve.collateral_amount,
                collateral_amount_contract
            )
            db_user_reserve.borrow_amount_live_is_verified = self.is_borrow_amount_verified(
                db_user_reserve.borrow_amount,
                borrow_amount_contract
            )

            # If both balances are 0, mark for deletion
            if collateral_amount_contract == Decimal('0') and borrow_amount_contract == Decimal('0'):
                db_user_reserve.mark_for_deletion = True

            # If live balances are not verified, update the raw balances
            if not db_user_reserve.collateral_amount_live_is_verified:
                db_user_reserve.collateral_amount = (
                    db_user_reserve.get_unscaled_balance(collateral_amount_contract, type="collateral")
                )
            if not db_user_reserve.borrow_amount_live_is_verified:
                db_user_reserve.borrow_amount = (
                    db_user_reserve.get_unscaled_balance(borrow_amount_contract, type="borrow")
                )

            updated_batch.append(db_user_reserve)
        return updated_batch


VerifyBalancesTask = app.register_task(VerifyBalancesTask())


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

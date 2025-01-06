import logging
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List

from aave.models import Asset
from aave.price import PriceConfigurer
from aave.tasks import UpdateAssetMetadataTask, UpdateMissingLiquidityIndexTask
from blockchains.models import Event
from utils.constants import EVM_NULL_ADDRESS
from utils.encoding import add_0x_prefix

logger = logging.getLogger(__name__)


class BalanceUtils:

    def _update_existing_records(model_class, event, asset_id, users, balances, liquidity_indices):
        """Update existing balance records"""
        instances_to_update = model_class.objects.filter(
            network=event.network,
            protocol=event.protocol,
            address__in=users,
            asset_id=asset_id
        )

        update_instances = [
            model_class(
                id=instance.id,
                collateral_amount=instance.collateral_amount + balances[asset_id][instance.address],
                last_updated_liquidity_index=(
                    liquidity_indices[asset_id][instance.address]
                    if liquidity_indices is not None else None
                )
            ) for instance in instances_to_update
        ]

        if liquidity_indices is not None:
            fields_to_update = ['collateral_amount', 'last_updated_liquidity_index']
        else:
            fields_to_update = ['collateral_amount']

        model_class.objects.bulk_update(
            update_instances,
            fields=fields_to_update
        )
        logger.info(f"Updated {len(update_instances)} existing records for asset {asset_id}")
        return instances_to_update

    def _create_new_records(model_class, event, asset_id, new_addresses, balances, liquidity_indices):
        """Create new balance records"""
        new_instances = [
            model_class(
                network=event.network,
                protocol=event.protocol,
                address=address,
                asset_id=asset_id,
                collateral_amount=balances[asset_id][address],
                last_updated_liquidity_index=(
                    liquidity_indices[asset_id][address]
                    if liquidity_indices is not None else None
                )
            )
            for address in new_addresses
        ]
        model_class.objects.bulk_create(new_instances)
        logger.info(f"Created {len(new_instances)} new records for asset {asset_id}")


class aaveAdapter(BalanceUtils):

    @staticmethod
    def dedupe_logs(logs: List[Dict]) -> Dict:
        """
        Groups logs by address and returns only the most recent log for each address.
        Most recent is determined by highest block number, or highest transaction index within same block.
        """
        latest_logs = {}
        for log in logs:
            asset = log.args.asset
            if asset not in latest_logs or (
                log.blockNumber > latest_logs[asset].blockNumber
                or (
                    log.blockNumber == latest_logs[asset].blockNumber
                    and log.transactionIndex > latest_logs[asset].transactionIndex
                )
            ):
                latest_logs[asset] = log
        return latest_logs

    @classmethod
    def _bulk_create_and_update_metadata(cls, model_class, defaults_list, update_fields):
        """Helper method to handle bulk create and metadata update."""
        if len(defaults_list) > 0:
            model_class.objects.bulk_create(
                [model_class(**defaults) for defaults in defaults_list],
                update_conflicts=True,
                update_fields=update_fields,
                unique_fields=['asset', 'network', 'protocol']
            )
            UpdateAssetMetadataTask.delay()

    @classmethod
    def parse_CollateralConfigurationChanged(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        defaults_list = []

        # Get most recent logs only
        latest_logs = cls.dedupe_logs(logs)

        # Create defaults list from deduped logs
        for log in latest_logs.values():
            defaults_list.append({
                'asset': log.args.asset,
                'network': event.network,
                'protocol': event.protocol,
                'liquidation_threshold': Decimal(log.args.liquidationThreshold),
                'liquidation_bonus': Decimal(log.args.liquidationBonus)
            })

        cls._bulk_create_and_update_metadata(
            model_class,
            defaults_list,
            ['liquidation_threshold', 'liquidation_bonus']
        )

    @classmethod
    def parse_ReserveInitialized(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        defaults_list = []

        # Get most recent logs only
        latest_logs = cls.dedupe_logs(logs)

        # Create defaults list from deduped logs
        for log in latest_logs.values():
            defaults_list.append({
                'asset': log.args.asset,
                'network': event.network,
                'protocol': event.protocol,
                'atoken_address': log.args.aToken,
                'stable_debt_token_address': "0x0000000000000000000000000000000000000000",
                'variable_debt_token_address': log.args.variableDebtToken,
            })

        cls._bulk_create_and_update_metadata(
            model_class,
            defaults_list,
            [
                "atoken_address",
                "stable_debt_token_address",
                "variable_debt_token_address",
            ]
        )

    @classmethod
    def parse_AssetSourceUpdated(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        defaults_list = []

        # Get most recent logs only
        latest_logs = cls.dedupe_logs(logs)

        for log in latest_logs.values():
            defaults_list.append({
                'asset': log.args.asset,
                'network': event.network,
                'protocol': event.protocol,
                'pricesource': log.args.source.lower()
            })

        # Process records individually
        for defaults in defaults_list:
            try:
                record, created = model_class.objects.update_or_create(
                    asset=defaults['asset'],
                    network=defaults['network'],
                    protocol=defaults['protocol'],
                    defaults=defaults
                )

                # Configure price source
                price_configurer = PriceConfigurer(record)
                price_configurer.set_price_configuration()
            except Exception as e:
                logger.error(f"Failed to process record for {defaults['asset']}: {str(e)}")

    @classmethod
    def parse_BorrowableInIsolationChanged(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        defaults_list = []

        # Get most recent logs only
        latest_logs = cls.dedupe_logs(logs)

        for log in latest_logs.values():
            defaults_list.append({
                'asset': log.args.asset,
                'network': event.network,
                'protocol': event.protocol,
                'borrowable_in_isolation_mode': log.args.borrowable
            })

        cls._bulk_create_and_update_metadata(
            model_class,
            defaults_list,
            ['borrowable_in_isolation_mode']
        )

    @classmethod
    def parse_EModeAssetCategoryChanged(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        defaults_list = []

        # Get most recent logs only
        latest_logs = cls.dedupe_logs(logs)

        for log in latest_logs.values():
            defaults_list.append({
                'asset': log.args.asset,
                'network': event.network,
                'protocol': event.protocol,
                'emode_category': log.args.newCategoryId
            })

        cls._bulk_create_and_update_metadata(
            model_class,
            defaults_list,
            ['emode_category']
        )

    @classmethod
    def parse_ReservePaused(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        defaults_list = []

        # Get most recent logs only
        latest_logs = cls.dedupe_logs(logs)

        for log in latest_logs.values():
            defaults_list.append({
                'asset': log.args.asset,
                'network': event.network,
                'protocol': event.protocol,
                'is_reserve_paused': log.args.paused
            })

        cls._bulk_create_and_update_metadata(
            model_class,
            defaults_list,
            ['is_reserve_paused']
        )

    @classmethod
    def parse_ReserveFactorChanged(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        defaults_list = []

        latest_logs = cls.dedupe_logs(logs)

        for log in latest_logs.values():
            defaults_list.append({
                'asset': log.args.asset,
                'network': event.network,
                'protocol': event.protocol,
                'reserve_factor': Decimal(log.args.newReserveFactor)
            })

        cls._bulk_create_and_update_metadata(
            model_class,
            defaults_list,
            ['reserve_factor']
        )

    @classmethod
    def parse_ReserveFlashLoaning(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        defaults_list = []

        latest_logs = cls.dedupe_logs(logs)

        for log in latest_logs.values():
            defaults_list.append({
                'asset': log.args.asset,
                'network': event.network,
                'protocol': event.protocol,
                'reserve_is_flash_loan_enabled': log.args.enabled
            })

        cls._bulk_create_and_update_metadata(
            model_class,
            defaults_list,
            ['reserve_is_flash_loan_enabled']
        )

    @classmethod
    def parse_ReserveBorrowing(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        defaults_list = []

        latest_logs = cls.dedupe_logs(logs)

        for log in latest_logs.values():
            defaults_list.append({
                'asset': log.args.asset,
                'network': event.network,
                'protocol': event.protocol,
                'reserve_is_borrow_enabled': log.args.enabled
            })

        cls._bulk_create_and_update_metadata(
            model_class,
            defaults_list,
            ['reserve_is_borrow_enabled']
        )

    @classmethod
    def parse_ReserveFrozen(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        defaults_list = []

        latest_logs = cls.dedupe_logs(logs)

        for log in latest_logs.values():
            defaults_list.append({
                'asset': log.args.asset,
                'network': event.network,
                'protocol': event.protocol,
                'reserve_is_frozen': log.args.frozen
            })

        cls._bulk_create_and_update_metadata(
            model_class,
            defaults_list,
            ['reserve_is_frozen']
        )

    @classmethod
    def parse_AssetCollateralInEModeChanged(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        defaults_list = []

        latest_logs = cls.dedupe_logs(logs)

        for log in latest_logs.values():
            defaults_list.append({
                'asset': log.args.asset,
                'network': event.network,
                'protocol': event.protocol,
                'emode_category': log.args.categoryId,
                'emode_is_collateral': log.args.collateral
            })

        cls._bulk_create_and_update_metadata(
            model_class,
            defaults_list,
            ['emode_category', 'emode_is_collateral']
        )

    @classmethod
    def parse_AssetBorrowableInEModeChanged(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        defaults_list = []

        latest_logs = cls.dedupe_logs(logs)

        for log in latest_logs.values():
            defaults_list.append({
                'asset': log.args.asset,
                'network': event.network,
                'protocol': event.protocol,
                'emode_category': log.args.categoryId,
                'emode_is_borrowable': log.args.borrowable
            })

        cls._bulk_create_and_update_metadata(
            model_class,
            defaults_list,
            ['emode_category', 'emode_is_borrowable']
        )

    @classmethod
    def parse_EModeCategoryAdded(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()

        for log in logs:
            # Update all assets in this emode category
            model_class.objects.filter(
                network=event.network,
                protocol=event.protocol,
                emode_category=log.args.categoryId
            ).update(
                emode_liquidation_threshold=Decimal(log.args.liquidationThreshold),
                emode_liquidation_bonus=Decimal(log.args.liquidationBonus)
            )

    @classmethod
    def parse_PriceCapUpdated(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()

        for log in logs:
            model_class.objects.filter(
                network=event.network,
                protocol=event.protocol,
                pricesource=log.address
            ).update(
                max_cap=Decimal(log.args.priceCap),
            )

    @classmethod
    def parse_CapParametersUpdated(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()

        for log in logs:
            model_class.objects.filter(
                network=event.network,
                protocol=event.protocol,
                pricesource=log.address
            ).update(
                max_cap=Decimal(log.args.snapshotRatio),
            )

    @classmethod
    def parse_LiquidationCall(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        onchain_received_at = datetime.now(timezone.utc)

        protocol_name = event.protocol.name
        network_name = event.network.name

        instances = []
        for log in logs:
            collateral_asset = Asset.get_by_address(
                protocol_name=protocol_name,
                network_name=network_name,
                token_address=log.args.collateralAsset
            )
            debt_asset = Asset.get_by_address(
                protocol_name=protocol_name,
                network_name=network_name,
                token_address=log.args.debtAsset
            )

            liquidated_collateral_amount = Decimal(log.args.liquidatedCollateralAmount)
            debt_to_cover = Decimal(log.args.debtToCover)
            liquidated_collateral_amount_in_usd = (
                liquidated_collateral_amount * collateral_asset.price_in_usdt / collateral_asset.decimals
            )
            debt_to_cover_in_usd = (
                debt_to_cover * debt_asset.price_in_usdt / debt_asset.decimals
            )

            instances.append(model_class(
                network=event.network,
                protocol=event.protocol,
                user=log.args.user,
                debt_to_cover=debt_to_cover,
                liquidated_collateral_amount=liquidated_collateral_amount,
                liquidator=log.args.liquidator,
                block_height=log.blockNumber,
                transaction_hash=log.transactionHash,
                transaction_index=log.transactionIndex,
                onchain_received_at=onchain_received_at,
                collateral_asset=collateral_asset,
                debt_asset=debt_asset,
                liquidated_collateral_amount_in_usd=liquidated_collateral_amount_in_usd,
                debt_to_cover_in_usd=debt_to_cover_in_usd,
            ))
        model_class.objects.bulk_create(instances)

    @classmethod
    def parse_Mint(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        atoken_maps = dict(Asset.objects.values_list('atoken_address', 'id'))

        balances = defaultdict(lambda: defaultdict(lambda: Decimal("0.0")))
        liquidity_indices = defaultdict(dict)
        max_liquidity_indexes = defaultdict(lambda: Decimal("0.0"))
        for log in logs:
            asset_id = atoken_maps.get(log.address)
            balances[asset_id][log.args.onBehalfOf] += Decimal(log.args.value)
            liquidity_index = Decimal(log.args.index)
            liquidity_indices[asset_id][log.args.onBehalfOf] = liquidity_index
            if liquidity_index > max_liquidity_indexes[asset_id]:
                max_liquidity_indexes[asset_id] = liquidity_index

        for asset_id in balances:
            users = list(balances[asset_id].keys())

            instances_to_update = cls._update_existing_records(
                model_class=model_class,
                event=event,
                asset_id=asset_id,
                users=users,
                balances=balances,
                liquidity_indices=liquidity_indices
            )

            # Create new records for addresses not yet in database
            existing_addresses = set(instance.address for instance in instances_to_update)
            new_addresses = set(users) - existing_addresses

            if new_addresses:
                cls._create_new_records(
                    model_class=model_class,
                    event=event,
                    asset_id=asset_id,
                    new_addresses=new_addresses,
                    balances=balances,
                    liquidity_indices=liquidity_indices
                )

        Asset.objects.bulk_update(
            (
                Asset(
                    id=asset_id,
                    liquidity_index=max_liquidity_indexes[asset_id]
                )
                for asset_id in max_liquidity_indexes
            ),
            fields=['liquidity_index']
        )

    @classmethod
    def parse_Burn(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        atoken_maps = dict(Asset.objects.values_list('atoken_address', 'id'))

        balances = defaultdict(lambda: defaultdict(lambda: Decimal("0.0")))
        liquidity_indices = defaultdict(dict)
        max_liquidity_indexes = defaultdict(lambda: Decimal("0.0"))

        for log in logs:
            asset_id = atoken_maps.get(log.address)
            addr = getattr(log.args, 'from')
            balances[asset_id][addr] -= Decimal(log.args.value)
            liquidity_index = Decimal(log.args.index)
            liquidity_indices[asset_id][addr] = liquidity_index
            if liquidity_index > max_liquidity_indexes[asset_id]:
                max_liquidity_indexes[asset_id] = liquidity_index

        for asset_id in balances:
            users = list(balances[asset_id].keys())

            instances_to_update = cls._update_existing_records(
                model_class=model_class,
                event=event,
                asset_id=asset_id,
                users=users,
                balances=balances,
                liquidity_indices=liquidity_indices
            )

            # Create new records for addresses not yet in database
            existing_addresses = set(instance.address for instance in instances_to_update)
            new_addresses = set(users) - existing_addresses

            if new_addresses:
                cls._create_new_records(
                    model_class=model_class,
                    event=event,
                    asset_id=asset_id,
                    new_addresses=new_addresses,
                    balances=balances,
                    liquidity_indices=liquidity_indices
                )

        Asset.objects.bulk_update(
            (
                Asset(
                    id=asset_id,
                    liquidity_index=max_liquidity_indexes[asset_id]
                )
                for asset_id in max_liquidity_indexes
            ),
            fields=['liquidity_index']
        )

    @classmethod
    def parse_Transfer(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        atoken_maps = dict(Asset.objects.values_list('atoken_address', 'id'))

        balances = defaultdict(lambda: defaultdict(lambda: Decimal("0.0")))

        for log in logs:
            asset_id = atoken_maps.get(log.address)

            to_addr = add_0x_prefix(log.args._to)
            from_addr = add_0x_prefix(log.args._from)

            if (to_addr == EVM_NULL_ADDRESS) or (from_addr == EVM_NULL_ADDRESS):
                continue

            balances[asset_id][to_addr] += Decimal(log.args.value)
            balances[asset_id][from_addr] -= Decimal(log.args.value)

        for asset_id in balances:
            users = list(balances[asset_id].keys())

            instances_to_update = cls._update_existing_records(
                model_class=model_class,
                event=event,
                asset_id=asset_id,
                users=users,
                balances=balances,
                liquidity_indices=None
            )

            # Create new records for addresses not yet in database
            existing_addresses = set(instance.address for instance in instances_to_update)
            new_addresses = set(users) - existing_addresses

            if new_addresses:
                cls._create_new_records(
                    model_class=model_class,
                    event=event,
                    asset_id=asset_id,
                    new_addresses=new_addresses,
                    balances=balances,
                    liquidity_indices=None
                )

        UpdateMissingLiquidityIndexTask.delay()

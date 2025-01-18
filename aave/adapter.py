import logging
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List

from aave.models import AaveBalanceLog, Asset
from aave.price import PriceConfigurer
from aave.tasks import UpdateAssetMetadataTask
from blockchains.models import ApproximateBlockTimestamp, Event
from utils.constants import EVM_NULL_ADDRESS
from utils.encoding import add_0x_prefix
from utils.events import is_latest_log

logger = logging.getLogger(__name__)


class BalanceUtils:

    def _update_existing_records(
        model_class: List[AaveBalanceLog],
        event: Event,
        asset_id: int,
        users: List[str],
        balances: Dict[int, Dict[str, Dict[str, Decimal]]],
        liquidity_indices: Dict[int, Dict[str, Dict[str, Decimal]]]
    ):
        """Update existing balance records"""
        instances_to_update = model_class.objects.filter(
            network=event.network,
            protocol=event.protocol,
            address__in=users,
            asset_id=asset_id
        )

        collateral_update_instances = []
        borrow_update_instances = []

        for instance in instances_to_update:
            balance_data = balances[asset_id][instance.address]
            collateral_amount = balance_data.get('collateral', Decimal("0.0"))
            borrow_amount = balance_data.get('borrow', Decimal("0.0"))

            liquidity_data = liquidity_indices[asset_id][instance.address] if liquidity_indices else {}
            collateral_index = liquidity_data.get('collateral')
            borrow_index = liquidity_data.get('borrow')

            if collateral_index:
                instance.collateral_amount += collateral_amount
                instance.last_updated_collateral_liquidity_index = collateral_index
                collateral_update_instances.append(instance)

            if borrow_index:
                instance.borrow_amount += borrow_amount
                instance.last_updated_borrow_liquidity_index = borrow_index
                borrow_update_instances.append(instance)

        if collateral_update_instances:
            model_class.objects.bulk_update(
                collateral_update_instances,
                fields=['collateral_amount', 'last_updated_collateral_liquidity_index']
            )
            logger.info(f"Updated collateral for {len(collateral_update_instances)} records for asset {asset_id}")

        if borrow_update_instances:
            model_class.objects.bulk_update(
                borrow_update_instances,
                fields=['borrow_amount', 'last_updated_borrow_liquidity_index']
            )
            logger.info(f"Updated borrow for {len(borrow_update_instances)} records for asset {asset_id}")

        return list(set(collateral_update_instances + borrow_update_instances))

    def _create_new_records(model_class, event, asset_id, new_addresses, balances, liquidity_indices):
        """Create new balance records"""
        instances = []

        for address in new_addresses:
            balance_data = balances[asset_id][address]
            collateral_amount = balance_data.get('collateral', Decimal("0.0"))
            borrow_amount = balance_data.get('borrow', Decimal("0.0"))

            liquidity_data = liquidity_indices[asset_id][address] if liquidity_indices else {}
            collateral_index = liquidity_data.get('collateral')
            borrow_index = liquidity_data.get('borrow')

            instance = model_class(
                network=event.network,
                protocol=event.protocol,
                address=address,
                asset_id=asset_id
            )

            if collateral_index and collateral_index > 0:
                instance.collateral_amount = collateral_amount
                instance.collateral_amount_live = instance.get_scaled_balance("collateral")
                instance.last_updated_collateral_liquidity_index = collateral_index

            if borrow_index and borrow_index > 0:
                instance.borrow_amount = borrow_amount
                instance.borrow_amount_live = instance.get_scaled_balance("borrow")
                instance.last_updated_borrow_liquidity_index = borrow_index

            instances.append(instance)

        if instances:
            model_class.objects.bulk_create(instances)
            logger.info(f"Created {len(instances)} new records for asset {asset_id}")


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
            if asset not in latest_logs or is_latest_log(log, latest_logs[asset]):
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
        approximate_block_timestamp = ApproximateBlockTimestamp.objects.get(network=event.network)

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

            block_timestamp = approximate_block_timestamp.get_timestamps([log.blockNumber])[log.blockNumber]
            block_datetime = datetime.fromtimestamp(block_timestamp, timezone.utc)

            liquidated_collateral_amount = Decimal(log.args.liquidatedCollateralAmount)
            debt_to_cover = Decimal(log.args.debtToCover)

            try:
                liquidated_collateral_amount_in_usd = (
                    liquidated_collateral_amount * collateral_asset.price_in_usdt / collateral_asset.decimals
                )
                debt_to_cover_in_usd = (
                    debt_to_cover * debt_asset.price_in_usdt / debt_asset.decimals
                )
                collateral_returned = (
                    liquidated_collateral_amount / collateral_asset.liquidation_bonus * Decimal("10000")
                )
                profit_in_usd = (
                    (liquidated_collateral_amount - collateral_returned) * collateral_asset.price_in_usdt
                    / collateral_asset.decimals
                )
            except Exception:
                liquidated_collateral_amount_in_usd = None
                debt_to_cover_in_usd = None
                profit_in_usd = None

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
                block_datetime=block_datetime,
                profit_in_usd=profit_in_usd
            ))
        model_class.objects.bulk_create(instances)

    @classmethod
    def parse_Mint(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        atoken_maps = dict(Asset.objects.values_list('atoken_address', 'id'))
        variable_debt_token_maps = dict(
            Asset.objects.values_list('variable_debt_token_address', 'id')
        )

        balances, liquidity_indices, max_indices = cls._process_mint_burn_logs(
            logs=logs,
            atoken_maps=atoken_maps,
            variable_debt_token_maps=variable_debt_token_maps,
            is_mint=True
        )

        cls._handle_balance_and_index_updates(
            model_class=model_class,
            event=event,
            balances=balances,
            liquidity_indices=liquidity_indices,
            max_indices=max_indices
        )

    @classmethod
    def parse_Burn(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        atoken_maps = dict(Asset.objects.values_list('atoken_address', 'id'))
        variable_debt_token_maps = dict(
            Asset.objects.values_list('variable_debt_token_address', 'id')
        )

        balances, liquidity_indices, max_indices = cls._process_mint_burn_logs(
            logs=logs,
            atoken_maps=atoken_maps,
            variable_debt_token_maps=variable_debt_token_maps,
            is_mint=False
        )

        cls._handle_balance_and_index_updates(
            model_class=model_class,
            event=event,
            balances=balances,
            liquidity_indices=liquidity_indices,
            max_indices=max_indices
        )

    @classmethod
    def _process_mint_burn_logs(
        cls, logs: List[Dict], atoken_maps: Dict, variable_debt_token_maps: Dict, is_mint: bool
    ):
        balances = defaultdict(
            lambda: defaultdict(
                lambda: {'collateral': Decimal("0.0"), 'borrow': Decimal("0.0")}
            )
        )
        liquidity_indices = defaultdict(lambda: defaultdict(dict))
        max_indices = {
            'collateral': defaultdict(lambda: Decimal("0.0")),
            'borrow': defaultdict(lambda: Decimal("0.0"))
        }

        for log in logs:
            collateral_asset_id = atoken_maps.get(log.address)
            borrow_asset_id = variable_debt_token_maps.get(log.address)

            value = Decimal(log.args.value)
            if not is_mint:
                value = -value

            if collateral_asset_id:
                cls._update_collateral_indices(
                    log=log,
                    asset_id=collateral_asset_id,
                    balances=balances,
                    liquidity_indices=liquidity_indices,
                    max_indices=max_indices,
                    value=value
                )

            if borrow_asset_id:
                cls._update_borrow_indices(
                    log=log,
                    asset_id=borrow_asset_id,
                    balances=balances,
                    liquidity_indices=liquidity_indices,
                    max_indices=max_indices,
                    value=value
                )

        return balances, liquidity_indices, max_indices

    @staticmethod
    def _update_collateral_indices(
        log, asset_id, balances, liquidity_indices, max_indices, value
    ):
        user = log.args.onBehalfOf if hasattr(log.args, 'onBehalfOf') else getattr(log.args, 'from')
        asset = Asset.get_by_id(asset_id)
        balances[asset_id][user]['collateral'] += value / asset.decimals
        liquidity_index = Decimal(log.args.index)
        if liquidity_index > liquidity_indices[asset_id][user].get('collateral', Decimal("0.0")):
            liquidity_indices[asset_id][user]['collateral'] = liquidity_index
        if liquidity_index > max_indices['collateral'][asset_id]:
            max_indices['collateral'][asset_id] = liquidity_index

    @staticmethod
    def _update_borrow_indices(
        log, asset_id, balances, liquidity_indices, max_indices, value
    ):
        user = log.args.onBehalfOf if hasattr(log.args, 'onBehalfOf') else getattr(log.args, 'from')
        asset = Asset.get_by_id(asset_id)
        balances[asset_id][user]['borrow'] += value / asset.decimals
        liquidity_index = Decimal(log.args.index)
        if liquidity_index > liquidity_indices[asset_id][user].get('borrow', Decimal("0.0")):
            liquidity_indices[asset_id][user]['borrow'] = liquidity_index
        if liquidity_index > max_indices['borrow'][asset_id]:
            max_indices['borrow'][asset_id] = liquidity_index

    @classmethod
    def _handle_balance_and_index_updates(
        cls, model_class, event, balances, liquidity_indices, max_indices
    ):
        cls._update_asset_liquidity_indices(max_indices)

        for asset_id in balances:
            users = list(balances[asset_id].keys())
            cls._handle_balance_updates(
                model_class=model_class,
                event=event,
                asset_id=asset_id,
                users=users,
                balances=balances,
                liquidity_indices=liquidity_indices
            )

    @classmethod
    def _handle_balance_updates(
        cls, model_class, event, asset_id, users, balances, liquidity_indices
    ):
        instances_to_update = cls._update_existing_records(
            model_class=model_class,
            event=event,
            asset_id=asset_id,
            users=users,
            balances=balances,
            liquidity_indices=liquidity_indices
        )

        existing_addresses = {instance.address for instance in instances_to_update}
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

    @classmethod
    def _update_asset_liquidity_indices(cls, max_indices):
        all_asset_ids = set(
            max_indices['collateral'].keys() | max_indices['borrow'].keys()
        )
        existing_assets = Asset.objects.filter(id__in=all_asset_ids)

        collateral_updates = []
        borrow_updates = []

        for asset in existing_assets:
            if (
                asset.id in max_indices['collateral']
                and max_indices['collateral'][asset.id] > asset.collateral_liquidity_index
            ):
                asset.collateral_liquidity_index = max_indices['collateral'][asset.id]
                collateral_updates.append(asset)

            if (
                asset.id in max_indices['borrow']
                and max_indices['borrow'][asset.id] > asset.borrow_liquidity_index
            ):
                asset.borrow_liquidity_index = max_indices['borrow'][asset.id]
                borrow_updates.append(asset)

        if collateral_updates:
            Asset.objects.bulk_update(
                collateral_updates,
                fields=['collateral_liquidity_index']
            )

        if borrow_updates:
            Asset.objects.bulk_update(
                borrow_updates,
                fields=['borrow_liquidity_index']
            )

    @classmethod
    def parse_BalanceTransfer(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        atoken_maps = dict(Asset.objects.values_list('atoken_address', 'id'))

        balances = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: Decimal("0.0"))))
        liquidity_indices = defaultdict(dict)
        max_indices = {
            'collateral': defaultdict(lambda: Decimal("0.0")),
            'borrow': defaultdict(lambda: Decimal("0.0"))
        }

        for log in logs:
            asset_id = atoken_maps.get(log.address)
            asset = Asset.get_by_id(asset_id)

            to_addr = add_0x_prefix(log.args._to)
            from_addr = add_0x_prefix(log.args._from)

            if (to_addr == EVM_NULL_ADDRESS) or (from_addr == EVM_NULL_ADDRESS):
                continue

            collateral_liquidity_index = Decimal(log.args.index)

            if collateral_liquidity_index > max_indices['collateral'][asset_id]:
                max_indices['collateral'][asset_id] = collateral_liquidity_index

            liquidity_indices[asset_id][to_addr] = {'collateral': collateral_liquidity_index}
            liquidity_indices[asset_id][from_addr] = {'collateral': collateral_liquidity_index}

            balances[asset_id][to_addr]['collateral'] += Decimal(log.args.value) / asset.decimals
            balances[asset_id][from_addr]['collateral'] -= Decimal(log.args.value) / asset.decimals

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
            existing_addresses = {instance.address for instance in instances_to_update}
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

        # Update liquidity indices
        cls._update_asset_liquidity_indices(max_indices)

    @classmethod
    def parse_ReserveUsedAsCollateralDisabled(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        latest_collateral_disabled = defaultdict(dict)

        for log in logs:
            asset_id = log.args.reserve
            asset = Asset.get_by_address(
                protocol_name=event.protocol.name,
                network_name=event.network.name,
                token_address=asset_id
            )
            user = log.args.user

            if (
                user not in latest_collateral_disabled[asset_id]
                or is_latest_log(log, latest_collateral_disabled[asset_id][user])
            ):
                latest_collateral_disabled[asset_id][user] = log

        instances_to_update = []
        for asset_id in latest_collateral_disabled:
            users = list(latest_collateral_disabled[asset_id].keys())
            user_reserves = model_class.objects.filter(
                network=event.network,
                protocol=event.protocol,
                address__in=users,
                asset_id=asset.id
            )
            for user_reserve in user_reserves:
                log = latest_collateral_disabled[asset_id][user_reserve.address]
                block_number = log.blockNumber
                if (
                    user_reserve.collateral_is_enabled_updated_at_block
                    or block_number >= user_reserve.collateral_is_enabled_updated_at_block
                ):
                    user_reserve.collateral_is_enabled_updated_at_block = block_number
                    user_reserve.collateral_is_enabled = False
                    user_reserve.collateral_amount_live = Decimal("0.0")
                    user_reserve.collateral_amount_live_with_liquidation_threshold = Decimal("0.0")
                    instances_to_update.append(user_reserve)

        model_class.objects.bulk_update(
            instances_to_update,
            fields=[
                'collateral_is_enabled',
                'collateral_is_enabled_updated_at_block',
                'collateral_amount_live',
                'collateral_amount_live_with_liquidation_threshold'
            ],
            batch_size=1000
        )

    @classmethod
    def parse_ReserveUsedAsCollateralEnabled(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        latest_collateral_enabled = defaultdict(dict)

        for log in logs:
            asset_id = log.args.reserve
            asset = Asset.get_by_address(
                protocol_name=event.protocol.name,
                network_name=event.network.name,
                token_address=asset_id
            )
            user = log.args.user

            if (
                user not in latest_collateral_enabled[asset_id]
                or is_latest_log(log, latest_collateral_enabled[asset_id][user])
            ):
                latest_collateral_enabled[asset_id][user] = log

        instances_to_update = []
        for asset_id in latest_collateral_enabled:
            users = list(latest_collateral_enabled[asset_id].keys())
            user_reserves = model_class.objects.filter(
                network=event.network,
                protocol=event.protocol,
                address__in=users,
                asset_id=asset.id
            ).select_related('asset')

            for user_reserve in user_reserves:
                log = latest_collateral_enabled[asset_id][user_reserve.address]
                block_number = log.blockNumber
                if (
                    user_reserve.collateral_is_enabled_updated_at_block
                    or block_number >= user_reserve.collateral_is_enabled_updated_at_block
                ):
                    user_reserve.collateral_is_enabled_updated_at_block = block_number
                    user_reserve.collateral_is_enabled = True
                    user_reserve.collateral_amount_live = user_reserve.get_scaled_balance("collateral")

                    if user_reserve.emode_category == 0:
                        user_reserve.collateral_amount_live_with_liquidation_threshold = (
                            user_reserve.collateral_amount_live
                            * user_reserve.asset.liquidation_threshold
                        )
                    else:
                        emode_liquidation_threshold = user_reserve.asset.emode_liquidation_threshold or Decimal("0.0")
                        user_reserve.collateral_amount_live_with_liquidation_threshold = (
                            user_reserve.collateral_amount_live
                            * emode_liquidation_threshold
                        )

                    instances_to_update.append(user_reserve)

        model_class.objects.bulk_update(
            instances_to_update,
            fields=[
                'collateral_is_enabled',
                'collateral_is_enabled_updated_at_block',
                'collateral_amount_live',
                'collateral_amount_live_with_liquidation_threshold'
            ],
            batch_size=1000
        )

    @classmethod
    def parse_UserEModeSet(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        latest_emode_categories = defaultdict(dict)

        for log in logs:
            user = log.args.user

            if user not in latest_emode_categories:
                latest_emode_categories[user] = log
            elif is_latest_log(log, latest_emode_categories[user]):
                latest_emode_categories[user] = log

        # Get existing records
        existing_users = set(model_class.objects.filter(
            network=event.network,
            protocol=event.protocol,
            address__in=list(latest_emode_categories.keys())
        ).values_list('address', flat=True))

        # Create new records for users that don't exist
        # new_instances = []
        # for user, user_data in latest_emode_categories.items():
        #     if user not in existing_users:
        #         new_instance = model_class(
        #             network=event.network,
        #             protocol=event.protocol,
        #             address=user,
        #             emode_category=user_data.args.categoryId,
        #             emode_category_updated_at_block=user_data.blockNumber
        #         )
        #         new_instances.append(new_instance)

        # if new_instances:
        #     model_class.objects.bulk_create(new_instances)

        # Update existing records
        instances_to_update = []
        user_reserves = model_class.objects.filter(
            network=event.network,
            protocol=event.protocol,
            address__in=existing_users
        ).select_related('asset')

        for user_reserve in user_reserves:
            user_data = latest_emode_categories[user_reserve.address]
            if (
                not user_reserve.emode_category_updated_at_block
                or user_data.blockNumber >= user_reserve.emode_category_updated_at_block
            ):
                user_reserve.emode_category = user_data.args.categoryId
                user_reserve.emode_category_updated_at_block = user_data.blockNumber
                user_reserve.collateral_amount_live = user_reserve.get_scaled_balance("collateral")

                if user_reserve.emode_category == 0:
                    user_reserve.collateral_amount_live_with_liquidation_threshold = (
                        user_reserve.collateral_amount_live
                        * user_reserve.asset.liquidation_threshold
                    )
                else:
                    emode_liquidation_threshold = user_reserve.asset.emode_liquidation_threshold or Decimal("0.0")
                    user_reserve.collateral_amount_live_with_liquidation_threshold = (
                        user_reserve.collateral_amount_live
                        * emode_liquidation_threshold
                    )

                instances_to_update.append(user_reserve)

        if instances_to_update:
            model_class.objects.bulk_update(
                instances_to_update,
                fields=[
                    'emode_category',
                    'emode_category_updated_at_block',
                    'collateral_amount_live',
                    'collateral_amount_live_with_liquidation_threshold'
                ],
                batch_size=1000
            )

import logging
from decimal import Decimal
from typing import Dict, List

from aave.price import PriceConfigurer
from aave.tasks import UpdateAssetMetadataTask
from blockchains.models import Event

logger = logging.getLogger(__name__)


class aaveAdapter:

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
                'stable_debt_token_address': log.args.stableDebtToken,
                'variable_debt_token_address': log.args.variableDebtToken,
                'interest_rate_strategy_address': log.args.interestRateStrategyAddress
            })

        cls._bulk_create_and_update_metadata(
            model_class,
            defaults_list,
            [
                "atoken_address",
                "stable_debt_token_address",
                "variable_debt_token_address",
                "interest_rate_strategy_address"
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
    def parse_ReserveInterestRateStrategyChanged(cls, event: Event, logs: List[Dict]):
        model_class = event.get_model_class()
        defaults_list = []

        latest_logs = cls.dedupe_logs(logs)

        for log in latest_logs.values():
            defaults_list.append({
                'asset': log.args.asset,
                'network': event.network,
                'protocol': event.protocol,
                'interest_rate_strategy_address': log.args.newStrategy
            })

        cls._bulk_create_and_update_metadata(
            model_class,
            defaults_list,
            ['interest_rate_strategy_address']
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

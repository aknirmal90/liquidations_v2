import os
from decimal import Decimal

from django.conf import settings
from web3 import Web3

from aave.models import Asset
from utils.encoding import get_method_id
from utils.files import parse_json


class PriceConfigurer:
    """
    Configures price source and related settings for an Aave asset
    """

    def load_abi(self):
        abi_file = os.path.join(os.path.dirname(settings.BASE_DIR), 'aave', 'price_abi.json')
        return parse_json(abi_file)

    def __init__(self, asset: Asset):
        """
        Initialize with an Asset instance

        Args:
            asset: Asset model instance to configure pricing for
        """
        bytecode = asset.network.rpc_adapter.get_bytecode(Web3.to_checksum_address(asset.pricesource))

        method_ids = []
        i = 0
        while i < len(bytecode):
            if bytecode[i:i + 2] == '63':
                method_id = "0x" + bytecode[i + 2:i + 10]
                if len(method_id) == 10:  # Ensure complete method ID
                    method_ids.append(method_id)
                i += 10
            else:
                i += 2

        self.method_ids = list(set(method_ids))
        self.abi = self.load_abi()
        self.asset = asset
        self.pricesource = asset.pricesource

    def set_price_configuration(self):
        def update_asset(response):
            non_null_fields = [(k, v) for k, v in response.items() if v is not None]
            for key, value in non_null_fields:
                setattr(self.asset, key, value)
            self.asset.save(update_fields=[k for k, v in non_null_fields])

        # Check GHO price
        gho_abi = self._get_function_abi('GHO_PRICE')
        if gho_abi and get_method_id(gho_abi) in self.method_ids:
            update_asset(self.handle_gho())
            return

        # Check aggregator price
        aggregator_abi = self._get_function_abi('aggregator')
        if aggregator_abi and get_method_id(aggregator_abi) in self.method_ids:
            update_asset(self.handle_aggregator())
            return

        # Check capped price
        capped_abi = self._get_function_abi('ASSET_TO_USD_AGGREGATOR')
        if capped_abi and get_method_id(capped_abi) in self.method_ids:
            update_asset(self.handle_capped_price())
            return

        # Check ratio price
        ratio_abi = self._get_function_abi('RATIO_PROVIDER')
        aggregator_abi = self._get_function_abi('BASE_TO_USD_AGGREGATOR')
        if (
            ratio_abi and get_method_id(ratio_abi) in self.method_ids
            and aggregator_abi and get_method_id(aggregator_abi) in self.method_ids
        ):
            update_asset(self.handle_ratio_price())
            return

    def _get_function_abi(self, function_name: str):
        for abi in self.abi:
            if abi['name'] == function_name and abi["type"] == "function":
                return abi
        return None

    def _call_contract_function(self, function_name: str, pricesource: str = None, *args):
        if pricesource is None:
            pricesource = self.pricesource

        contract = self.asset.network.rpc_adapter.client.eth.contract(
            address=Web3.to_checksum_address(pricesource),
            abi=self.abi
        )
        contract_function = getattr(contract.functions, function_name)
        return contract_function(*args).call()

    def handle_gho(self):
        constant_price = Decimal(self._call_contract_function('GHO_PRICE'))
        decimals = self._call_contract_function('decimals')

        return {
            'price_type': Asset.PriceType.CONSTANT,
            'contractA': None,
            'contractB': None,
            'numerator': Decimal(10 ** decimals),
            'denominator': Decimal('1.00'),
            'price': constant_price,
            'price_in_usdt': None
        }

    def handle_aggregator(self):
        contractA = self._call_contract_function('aggregator').lower()
        decimals = self._call_contract_function('decimals')

        return {
            'price_type': Asset.PriceType.AGGREGATOR,
            'contractA': contractA,
            'contractB': None,
            'numerator': Decimal(10 ** decimals),
            'denominator': Decimal('1.00'),
            'price': None,
            'price_in_usdt': None
        }

    def handle_capped_price(self):
        aggregator_contract = self._call_contract_function('ASSET_TO_USD_AGGREGATOR').lower()
        contractA = self._call_contract_function('aggregator', aggregator_contract).lower()
        decimals = self._call_contract_function('decimals')

        return {
            'price_type': Asset.PriceType.MAX_CAPPED,
            'contractA': contractA,
            'contractB': None,
            'numerator': Decimal(10 ** decimals),
            'denominator': Decimal('1.00'),
            'price': None,
            'price_in_usdt': None
        }

    def handle_ratio_price(self):
        aggregator_contractA = self._call_contract_function('BASE_TO_USD_AGGREGATOR').lower()
        contractA = self._call_contract_function('aggregator', aggregator_contractA).lower()

        aggregator_contractB = self._call_contract_function('RATIO_PROVIDER').lower()
        contractB = self._call_contract_function('aggregator', aggregator_contractB).lower()
        decimals = self._call_contract_function('decimals')
        ratio_decimals = self._call_contract_function('RATIO_DECIMALS')

        return {
            'price_type': Asset.PriceType.RATIO,
            'contractA': contractA,
            'contractB': contractB,
            'numerator': Decimal(10 ** decimals),
            'denominator': Decimal(10 ** ratio_decimals),
            'price': None,
            'price_in_usdt': None
        }

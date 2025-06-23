from oracles.contracts.AggregatorProxy import AggregatorProxyAssetSource
from oracles.contracts.base import BaseEthereumAssetSource
from oracles.contracts.CbETHPriceCapAdapter import CbETHPriceCapAdapterAssetSource
from oracles.contracts.CLrETHSynchronicityPriceAdapter import (
    CLrETHSynchronicityPriceAdapterAssetSource,
)
from oracles.contracts.CLSynchronicityPriceAdapterPegToBase import (
    CLSynchronicityPriceAdapterPegToBaseAssetSource,
)
from oracles.contracts.CLwstETHSynchronicityPriceAdapter import (
    CLwstETHSynchronicityPriceAdapterAssetSource,
)
from oracles.contracts.GhoOracle import GhoOracleAssetSource
from oracles.contracts.OsETHPriceCapAdapter import OsETHPriceCapAdapterAssetSource
from oracles.contracts.PendlePriceCapAdapter import PendlePriceCapAdapterAssetSource
from oracles.contracts.PriceCapAdapterStable import PriceCapAdapterStableAssetSource
from oracles.contracts.RETHPriceCapAdapter import RETHPriceCapAdapterAssetSource
from oracles.contracts.RsETHPriceCapAdapter import RsETHPriceCapAdapterAssetSource
from oracles.contracts.sDAIMainnetPriceCapAdapter import (
    sDAIMainnetPriceCapAdapterAssetSource,
)
from oracles.contracts.sDAISynchronicityPriceAdapter import (
    sDAISynchronicityPriceAdapterAssetSource,
)
from oracles.contracts.SUSDePriceCapAdapter import SUSDePriceCapAdapterAssetSource
from oracles.contracts.WeETHPriceCapAdapter import WeETHPriceCapAdapterAssetSource
from oracles.contracts.WstETHPriceCapAdapter import WstETHPriceCapAdapterAssetSource
from oracles.contracts.WstETHSynchronicityPriceAdapter import (
    WstETHSynchronicityPriceAdapterAssetSource,
)


class UnsupportedAssetSourceError(Exception):
    pass


def get_contract_interface(asset, asset_source):
    base_source = BaseEthereumAssetSource(asset=asset, asset_source=asset_source)
    if base_source.name == "EACAggregatorProxy":
        return AggregatorProxyAssetSource(asset=asset, asset_source=asset_source)
    elif base_source.name == "PriceCapAdapterStable":
        return PriceCapAdapterStableAssetSource(asset=asset, asset_source=asset_source)
    elif base_source.name == "PendlePriceCapAdapter":
        return PendlePriceCapAdapterAssetSource(asset=asset, asset_source=asset_source)
    elif base_source.name == "CLSynchronicityPriceAdapterPegToBase":
        return CLSynchronicityPriceAdapterPegToBaseAssetSource(
            asset=asset, asset_source=asset_source
        )
    elif base_source.name == "CbETHPriceCapAdapter":
        return CbETHPriceCapAdapterAssetSource(asset=asset, asset_source=asset_source)
    elif base_source.name in ("RETHPriceCapAdapter", "EthXPriceCapAdapter"):
        return RETHPriceCapAdapterAssetSource(asset=asset, asset_source=asset_source)
    elif base_source.name == "WstETHPriceCapAdapter":
        return WstETHPriceCapAdapterAssetSource(asset=asset, asset_source=asset_source)
    elif base_source.name in ("WeETHPriceCapAdapter", "EBTCPriceCapAdapter"):
        return WeETHPriceCapAdapterAssetSource(asset=asset, asset_source=asset_source)
    elif base_source.name in ("OsETHPriceCapAdapter", "EUSDePriceCapAdapter"):
        return OsETHPriceCapAdapterAssetSource(asset=asset, asset_source=asset_source)
    elif base_source.name == "RsETHPriceCapAdapter":
        return RsETHPriceCapAdapterAssetSource(asset=asset, asset_source=asset_source)
    elif base_source.name == "sDAIMainnetPriceCapAdapter":
        return sDAIMainnetPriceCapAdapterAssetSource(
            asset=asset, asset_source=asset_source
        )
    elif base_source.name == "SUSDePriceCapAdapter":
        return SUSDePriceCapAdapterAssetSource(asset=asset, asset_source=asset_source)
    elif base_source.name == "CLwstETHSynchronicityPriceAdapter":
        return CLwstETHSynchronicityPriceAdapterAssetSource(
            asset=asset, asset_source=asset_source
        )
    elif base_source.name == "CLrETHSynchronicityPriceAdapter":
        return CLrETHSynchronicityPriceAdapterAssetSource(
            asset=asset, asset_source=asset_source
        )
    elif base_source.name == "WstETHSynchronicityPriceAdapter":
        return WstETHSynchronicityPriceAdapterAssetSource(
            asset=asset, asset_source=asset_source
        )
    elif base_source.name == "sDAISynchronicityPriceAdapter":
        return sDAISynchronicityPriceAdapterAssetSource(
            asset=asset, asset_source=asset_source
        )
    elif base_source.name == "GhoOracle":
        return GhoOracleAssetSource(asset=asset, asset_source=asset_source)
    else:
        raise UnsupportedAssetSourceError(f"Invalid asset source: {base_source.name}")

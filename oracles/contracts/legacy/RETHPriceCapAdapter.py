from oracles.contracts.PriceCapAdapter import PriceCapAdapterAssetSource


class RETHPriceCapAdapterAssetSource(PriceCapAdapterAssetSource):
    @property
    def RATIO_PROVIDER_METHOD(self):
        return "getExchangeRate"

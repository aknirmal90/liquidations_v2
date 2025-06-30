from oracles.contracts.PriceCapAdapter import PriceCapAdapterAssetSource


class CbETHPriceCapAdapterAssetSource(PriceCapAdapterAssetSource):
    @property
    def RATIO_PROVIDER_METHOD(self):
        return "exchangeRate"

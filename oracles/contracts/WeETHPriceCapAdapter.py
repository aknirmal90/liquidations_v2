from oracles.contracts.PriceCapAdapter import PriceCapAdapterAssetSource


class WeETHPriceCapAdapterAssetSource(PriceCapAdapterAssetSource):
    @property
    def RATIO_PROVIDER_METHOD(self):
        return "getRate"

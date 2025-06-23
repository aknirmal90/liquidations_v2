from oracles.contracts.PriceCapAdapter import PriceCapAdapterAssetSource


class sDAIMainnetPriceCapAdapterAssetSource(PriceCapAdapterAssetSource):
    @property
    def RATIO_PROVIDER_METHOD(self):
        return "chi"

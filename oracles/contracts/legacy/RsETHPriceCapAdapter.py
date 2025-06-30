from oracles.contracts.PriceCapAdapter import PriceCapAdapterAssetSource


class RsETHPriceCapAdapterAssetSource(PriceCapAdapterAssetSource):
    @property
    def RATIO_PROVIDER_METHOD(self):
        return "rsETHPrice"

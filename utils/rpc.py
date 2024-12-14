from django.core.cache import cache
from blockchains.models import Network


class EVMRpcAdapter:
    def __init__(self, network_name: str):
        network = Network.get(network_name)
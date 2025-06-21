from collections import OrderedDict
from datetime import datetime

import requests
from decouple import config

from utils.constants import NETWORK_NAME


def token_metadata(token_address: str) -> dict:
    if NETWORK_NAME != "ethereum":
        raise ValueError(f"Network {NETWORK_NAME} is not supported for token metadata")

    url = f"https://eth-mainnet.g.alchemy.com/v2/{config('ALCHEMY_API_KEY')}"
    headers = {"accept": "application/json", "content-type": "application/json"}
    data = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "alchemy_getTokenMetadata",
        "params": [token_address],
    }

    response = requests.post(url, headers=headers, json=data)
    result = response.json()["result"]

    tokenName = result["name"]
    symbol = result["symbol"]
    decimals = result["decimals"]

    return OrderedDict(
        [
            ("asset", token_address.lower()),
            ("name", tokenName),
            ("symbol", symbol),
            ("decimals_places", 10**decimals),
            ("decimals", decimals),
            ("createdAt", int(datetime.now().timestamp() * 1_000_000)),
        ]
    )


def get_token_metadata_clickhouse_schema() -> dict:
    return [
        ("asset", "String"),
        ("name", "String"),
        ("symbol", "String"),
        ("decimals_places", "UInt64"),
        ("decimals", "UInt64"),
        ("createdAt", "UInt64"),
    ]

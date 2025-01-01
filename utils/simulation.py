from decimal import Decimal

import requests
from decouple import config

from config.models import Configuration


def get_tenderly_simulation_response(
    chain_id,
    from_address,
    to_address,
    input,
    value,
    gas=None,
    block_number=None,
    transaction_index=None,
    simulation_type="abi",
):
    headers = {
        "X-Access-Key": config("TENDERLY_APIKEY"),
        "content-type": "application/json",
    }

    if gas:
        gas = gas if isinstance(gas, int) else int(str(gas), 16)

    json_data = {
        "network_id": str(chain_id),
        "from": from_address,
        "to": to_address,
        "input": input,
        "gas": gas,
        "value": str(value),
        "save": False,
        "save_if_fails": False,
        "simulation_type": simulation_type,
        "block_number": block_number,
        "transaction_index": transaction_index,
    }

    response = requests.post(
        "https://api.tenderly.co/api/v1/account/aknirmal90/project/test/simulate",
        headers=headers,
        json=json_data,
    ).json()
    return response


def get_simulated_health_factor(
    chain_id,
    block_number,
    address,
    transaction_index,
):
    # import ipdb; ipdb.set_trace()
    key = f"AAVE_POOL_CONTRACT_{chain_id}"
    configuration = Configuration.objects.get(key=key)

    method_id = "0xbf92857c"  # getHealthFactor(address)
    input = method_id + address.replace("0x", "").zfill(64)

    response = get_tenderly_simulation_response(
        chain_id=chain_id,
        from_address=configuration.value,
        to_address=configuration.value,
        input=input,
        value=0,
        block_number=block_number,
        transaction_index=transaction_index,
    )

    for item in response["transaction"]["transaction_info"]["call_trace"]['decoded_output']:
        if item['soltype']['name'] == 'healthFactor':
            return Decimal(item['value']) / Decimal(10 ** 18)
    return None

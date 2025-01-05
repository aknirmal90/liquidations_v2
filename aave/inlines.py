import web3
from django_admin_inline_paginator.admin import TabularInlinePaginated
from web3._utils.events import get_event_data

from aave.models import (
    AaveBalanceLog,
    AaveBurnEvent,
    AaveMintEvent,
    AaveSupplyEvent,
    AaveTransferEvent,
    AaveWithdrawEvent,
)
from blockchains.models import Event
from config.models import Configuration
from utils.admin import get_explorer_address_url, get_explorer_transaction_url
from utils.constants import EVM_NULL_ADDRESS
from utils.encoding import add_0x_prefix, get_topic_0


class AaveMintEventInline(TabularInlinePaginated):
    model = AaveMintEvent
    extra = 0
    can_delete = False
    can_add = False
    fields = [
        'caller_link', 'on_behalf_of_link', 'value', 'balance_increase', 'transaction_hash_link',
        'block_height', 'transaction_index', 'log_index'
    ]
    readonly_fields = fields
    ordering = ('-block_height', '-transaction_index', '-log_index')
    per_page = 5
    classes = ['collapse']

    def caller_link(self, obj):
        return get_explorer_address_url(obj.balance_log.network, obj.caller)
    caller_link.short_description = 'Caller'

    def on_behalf_of_link(self, obj):
        return get_explorer_address_url(obj.balance_log.network, obj.on_behalf_of)
    on_behalf_of_link.short_description = 'On Behalf Of'

    def transaction_hash_link(self, obj):
        return get_explorer_transaction_url(obj.balance_log.network, obj.transaction_hash)
    transaction_hash_link.short_description = 'Transaction Hash'


class AaveBurnEventInline(TabularInlinePaginated):
    model = AaveBurnEvent
    extra = 0
    can_delete = False
    can_add = False
    fields = [
        'target_link', 'from_address_link', 'value', 'balance_increase', 'transaction_hash_link',
        'block_height', 'transaction_index', 'log_index'
    ]
    readonly_fields = fields
    ordering = ('-block_height', '-transaction_index', '-log_index')
    per_page = 5
    classes = ['collapse']

    def from_address_link(self, obj):
        return get_explorer_address_url(obj.balance_log.network, obj.from_address)
    from_address_link.short_description = 'From Address'

    def target_link(self, obj):
        return get_explorer_address_url(obj.balance_log.network, obj.target)
    target_link.short_description = 'Target'

    def transaction_hash_link(self, obj):
        return get_explorer_transaction_url(obj.balance_log.network, obj.transaction_hash)
    transaction_hash_link.short_description = 'Transaction Hash'


class AaveTransferEventInline(TabularInlinePaginated):
    model = AaveTransferEvent
    extra = 0
    can_delete = False
    can_add = False
    fields = [
        'from_address_link', 'to_address_link', 'value',
        'transaction_hash_link', 'block_height', 'transaction_index', 'log_index'
    ]
    readonly_fields = fields
    ordering = ('-block_height', '-transaction_index', '-log_index')
    per_page = 5
    classes = ['collapse']

    def from_address_link(self, obj):
        return get_explorer_address_url(obj.balance_log.network, obj.from_address)
    from_address_link.short_description = 'From Address'

    def to_address_link(self, obj):
        return get_explorer_address_url(obj.balance_log.network, obj.to_address)
    to_address_link.short_description = 'To Address'

    def transaction_hash_link(self, obj):
        return get_explorer_transaction_url(obj.balance_log.network, obj.transaction_hash)
    transaction_hash_link.short_description = 'Transaction Hash'


class AaveSupplyEventInline(TabularInlinePaginated):
    model = AaveSupplyEvent
    extra = 0
    can_delete = False
    can_add = False
    fields = [
        'user_link', 'on_behalf_of_link', 'amount', 'referral_code',
        'transaction_hash_link', 'block_height', 'transaction_index', 'log_index'
    ]
    readonly_fields = fields
    ordering = ('-block_height', '-transaction_index', '-log_index')
    per_page = 5
    classes = ['collapse']

    def user_link(self, obj):
        return get_explorer_address_url(obj.balance_log.network, obj.user)
    user_link.short_description = 'User'

    def on_behalf_of_link(self, obj):
        return get_explorer_address_url(obj.balance_log.network, obj.on_behalf_of)
    on_behalf_of_link.short_description = 'On Behalf Of'

    def transaction_hash_link(self, obj):
        return get_explorer_transaction_url(obj.balance_log.network, obj.transaction_hash)
    transaction_hash_link.short_description = 'Transaction Hash'


class AaveWithdrawEventInline(TabularInlinePaginated):
    model = AaveWithdrawEvent
    extra = 0
    can_delete = False
    can_add = False
    fields = [
        'user_link', 'to_address_link', 'amount',
        'transaction_hash_link', 'block_height', 'transaction_index', 'log_index'
    ]
    readonly_fields = fields
    ordering = ('-block_height', '-transaction_index', '-log_index')
    per_page = 5
    classes = ['collapse']

    def user_link(self, obj):
        return get_explorer_address_url(obj.balance_log.network, obj.user)
    user_link.short_description = 'User'

    def to_address_link(self, obj):
        return get_explorer_address_url(obj.balance_log.network, obj.to_address)
    to_address_link.short_description = 'To Address'

    def transaction_hash_link(self, obj):
        return get_explorer_transaction_url(obj.balance_log.network, obj.transaction_hash)
    transaction_hash_link.short_description = 'Transaction Hash'


def address_to_topic(address: str):
    address = address[2:] if address.startswith('0x') else address
    return add_0x_prefix(address.zfill(64))


def get_burn_events_for_address(balance_log: AaveBalanceLog):
    event = Event.objects.get(
        protocol=balance_log.protocol,
        network=balance_log.network,
        name="Burn"
    )
    rpc_adapter = event.network.rpc_adapter
    atoken = balance_log.asset.atoken_address
    AaveBurnEvent.objects.filter(balance_log=balance_log).delete()

    burn_events = rpc_adapter.client.eth.get_logs(
        {
            "topics": [
                event.topic_0,
                address_to_topic(balance_log.address),
                None
            ],
            "address": web3.Web3.to_checksum_address(atoken),
            "fromBlock": "0x0",
            "toBlock": hex(event.network.latest_block)
        }
    )
    burn_event_objects = []
    for log in burn_events:
        event_data = get_event_data(web3.Web3().codec, event.abi, log)
        burn_event_objects.append(AaveBurnEvent(
            from_address=event_data["args"]["from"],
            target=event_data["args"]["target"],
            value=event_data["args"]["value"],
            balance_increase=event_data["args"]["balanceIncrease"],
            index=event_data["args"]["index"],
            transaction_hash=add_0x_prefix(event_data['transactionHash']),
            transaction_index=event_data['transactionIndex'],
            log_index=event_data['logIndex'],
            block_height=event_data['blockNumber'],
            balance_log=balance_log
        ))

    AaveBurnEvent.objects.bulk_create(
        burn_event_objects,
        update_conflicts=True,
        unique_fields=['transaction_hash', 'block_height', 'log_index', 'balance_log'],
        update_fields=['from_address', 'target', 'value', 'balance_increase', 'index', 'transaction_index']
    )


def get_mint_events_for_address(balance_log: AaveBalanceLog):
    event = Event.objects.get(
        protocol=balance_log.protocol,
        network=balance_log.network,
        name="Mint"
    )
    rpc_adapter = event.network.rpc_adapter
    atoken = balance_log.asset.atoken_address
    AaveMintEvent.objects.filter(balance_log=balance_log).delete()

    mint_events = rpc_adapter.client.eth.get_logs(
        {
            "topics": [
                [event.topic_0],
                None,
                [address_to_topic(balance_log.address)]
            ],
            "address": web3.Web3.to_checksum_address(atoken),
            "fromBlock": "0x0",
            "toBlock": hex(event.network.latest_block)
        }
    )
    mint_event_objects = []
    for log in mint_events:
        event_data = get_event_data(web3.Web3().codec, event.abi, log)
        mint_event_objects.append(AaveMintEvent(
            caller=event_data["args"]["caller"],
            on_behalf_of=event_data["args"]["onBehalfOf"],
            value=event_data["args"]["value"],
            balance_increase=event_data["args"]["balanceIncrease"],
            index=event_data["args"]["index"],
            transaction_hash=add_0x_prefix(event_data['transactionHash']),
            transaction_index=event_data['transactionIndex'],
            log_index=event_data['logIndex'],
            block_height=event_data['blockNumber'],
            balance_log=balance_log
        ))

    AaveMintEvent.objects.bulk_create(
        mint_event_objects,
        update_conflicts=True,
        unique_fields=['transaction_hash', 'block_height', 'log_index', 'balance_log'],
        update_fields=['caller', 'on_behalf_of', 'value', 'balance_increase', 'index', 'transaction_index']
    )


def get_transfer_events_for_address(balance_log: AaveBalanceLog):
    event = Event.objects.get(
        protocol=balance_log.protocol,
        network=balance_log.network,
        name="Transfer"
    )
    rpc_adapter = event.network.rpc_adapter
    atoken = balance_log.asset.atoken_address
    AaveTransferEvent.objects.filter(balance_log=balance_log).delete()

    transfer_events = rpc_adapter.client.eth.get_logs(
        {
            "topics": [
                [event.topic_0],
                None,
                [address_to_topic(balance_log.address)]
            ],
            "address": web3.Web3.to_checksum_address(atoken),
            "fromBlock": "0x0",
            "toBlock": hex(event.network.latest_block)
        }
    )
    transfer_event_objects = []
    for log in transfer_events:
        event_data = get_event_data(web3.Web3().codec, event.abi, log)
        from_address = event_data["args"]["_from"]
        to_address = event_data["args"]["_to"]

        if from_address == EVM_NULL_ADDRESS or to_address == EVM_NULL_ADDRESS:
            continue

        transfer_event_objects.append(AaveTransferEvent(
            from_address=event_data["args"]["_from"],
            to_address=event_data["args"]["_to"],
            value=event_data["args"]["value"],
            transaction_hash=add_0x_prefix(event_data['transactionHash']),
            transaction_index=event_data['transactionIndex'],
            log_index=event_data['logIndex'],
            block_height=event_data['blockNumber'],
            balance_log=balance_log
        ))

    AaveTransferEvent.objects.bulk_create(
        transfer_event_objects,
        update_conflicts=True,
        unique_fields=['transaction_hash', 'block_height', 'log_index', 'balance_log'],
        update_fields=['from_address', 'to_address', 'value', 'transaction_index']
    )


def get_supply_events_for_address(balance_log: AaveBalanceLog):
    event_abi = balance_log.protocol.get_evm_event_abi("Supply")
    event_topic_0 = get_topic_0(event_abi)
    rpc_adapter = balance_log.network.rpc_adapter
    AaveSupplyEvent.objects.filter(balance_log=balance_log).delete()
    configuration = Configuration.get(f"AAVE_POOL_CONTRACT_{balance_log.network.chain_id}")
    supply_events = rpc_adapter.client.eth.get_logs(
        {
            "topics": [
                [event_topic_0],
                None,
                [address_to_topic(balance_log.address)]
            ],
            "address": web3.Web3.to_checksum_address(configuration),
            "fromBlock": "0x0",
            "toBlock": hex(balance_log.network.latest_block)
        }
    )
    supply_event_objects = []
    for log in supply_events:
        event_data = get_event_data(web3.Web3().codec, event_abi, log)
        supply_event_objects.append(AaveSupplyEvent(
            user=event_data["args"]["user"],
            on_behalf_of=event_data["args"]["onBehalfOf"],
            amount=event_data["args"]["amount"],
            referral_code=event_data["args"]["referralCode"],
            transaction_hash=add_0x_prefix(event_data['transactionHash']),
            transaction_index=event_data['transactionIndex'],
            log_index=event_data['logIndex'],
            block_height=event_data['blockNumber'],
            balance_log=balance_log
        ))

    AaveSupplyEvent.objects.bulk_create(
        supply_event_objects,
        update_conflicts=True,
        unique_fields=['transaction_hash', 'block_height', 'log_index', 'balance_log'],
        update_fields=['user', 'on_behalf_of', 'amount', 'referral_code', 'transaction_index']
    )


def get_withdraw_events_for_address(balance_log: AaveBalanceLog):
    event_abi = balance_log.protocol.get_evm_event_abi("Withdraw")
    event_topic_0 = get_topic_0(event_abi)
    rpc_adapter = balance_log.network.rpc_adapter
    AaveWithdrawEvent.objects.filter(balance_log=balance_log).delete()
    configuration = Configuration.get(f"AAVE_POOL_CONTRACT_{balance_log.network.chain_id}")

    withdraw_events = rpc_adapter.client.eth.get_logs(
        {
            "topics": [
                [event_topic_0],
                None,
                [address_to_topic(balance_log.address)]
            ],
            "address": web3.Web3.to_checksum_address(configuration),
            "fromBlock": "0x0",
            "toBlock": hex(balance_log.network.latest_block)
        }
    )
    withdraw_event_objects = []
    for log in withdraw_events:
        event_data = get_event_data(web3.Web3().codec, event_abi, log)
        withdraw_event_objects.append(AaveWithdrawEvent(
            user=event_data["args"]["user"],
            to_address=event_data["args"]["to"],
            amount=event_data["args"]["amount"],
            transaction_hash=add_0x_prefix(event_data['transactionHash']),
            transaction_index=event_data['transactionIndex'],
            log_index=event_data['logIndex'],
            block_height=event_data['blockNumber'],
            balance_log=balance_log
        ))

    AaveWithdrawEvent.objects.bulk_create(
        withdraw_event_objects,
        update_conflicts=True,
        unique_fields=['transaction_hash', 'block_height', 'log_index', 'balance_log'],
        update_fields=['user', 'to_address', 'amount', 'transaction_index']
    )
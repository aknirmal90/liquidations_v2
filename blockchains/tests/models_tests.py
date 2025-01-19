import pytest
from django.test import TestCase

from blockchains.models import ApproximateBlockTimestamp, Contract, Event, Network, Protocol


@pytest.mark.django_db
class TestProtocolModel(TestCase):
    def test_protocol_defaults(self):
        protocol = Protocol.objects.create(name="test_protocol")
        assert protocol.name == "test_protocol"
        assert protocol.is_enabled is False
        assert protocol.config_path == "test_protocol/config.yaml"

    def test_protocol_str(self):
        protocol = Protocol.objects.create(name="test_protocol")
        assert str(protocol) == "test_protocol"


@pytest.mark.django_db
class TestNetworkModel(TestCase):
    def test_network_defaults(self):
        network = Network.objects.create(name="test_network")
        assert network.name == "test_network"
        assert network.rpc is None
        assert network.chain_id is None
        assert network.wss_infura is None
        assert network.wss_sequencer_oregon is None
        assert network.latest_block == 0

    def test_network_str(self):
        network = Network.objects.create(name="test_network")
        assert str(network) == "test_network"


@pytest.mark.django_db
class TestEventModel(TestCase):
    def setUp(self):
        self.protocol = Protocol.objects.create(name="test_protocol")
        self.network = Network.objects.create(name="test_network")
        self.event = Event.objects.create(
            network=self.network,
            protocol=self.protocol,
            name="test_event",
            signature="test_signature",
            abi={},
            topic_0="test_topic"
        )

    def test_event_defaults(self):
        assert self.event.last_synced_block == 0
        assert self.event.is_enabled is False
        assert self.event.model_class is None
        assert self.event.contract_addresses is None

    def test_event_str(self):
        assert str(self.event) == "test_event - test_network"

    def test_blocks_to_sync_with_none_values(self):
        self.event.last_synced_block = None
        assert self.event.blocks_to_sync is None


@pytest.mark.django_db
class TestContractModel(TestCase):
    def setUp(self):
        self.protocol = Protocol.objects.create(name="test_protocol")
        self.network = Network.objects.create(name="test_network")
        self.contract = Contract.objects.create(
            contract_address="0x123",
            network=self.network,
            protocol=self.protocol
        )

    def test_contract_defaults(self):
        assert self.contract.is_enabled is True
        assert self.contract.contract_address == "0x123"

    def test_contract_str(self):
        assert str(self.contract) == "0x123 for test_protocol on test_network"


@pytest.mark.django_db
class TestApproximateBlockTimestampModel(TestCase):
    def setUp(self):
        self.network = Network.objects.create(name="test_network")

    def test_approximate_block_timestamp_defaults(self):
        block_timestamp = ApproximateBlockTimestamp.objects.create(
            network=self.network,
            reference_block_number=1000
        )
        assert block_timestamp.timestamp is None
        assert block_timestamp.block_time_in_milliseconds is None

    def test_approximate_block_timestamp_str(self):
        block_timestamp = ApproximateBlockTimestamp.objects.create(
            network=self.network,
            reference_block_number=1000
        )
        assert str(block_timestamp) == "test_network - 1000"

    def test_get_timestamps(self):
        block_timestamp = ApproximateBlockTimestamp.objects.create(
            network=self.network,
            reference_block_number=1000,
            timestamp=1500000000,
            block_time_in_milliseconds=15000  # 15 seconds
        )

        blocks = [999, 1000, 1001]
        timestamps = block_timestamp.get_timestamps(blocks)

        assert timestamps[999] == 1499999985  # 1500000000 - 15
        assert timestamps[1000] == 1500000000
        assert timestamps[1001] == 1500000015  # 1500000000 + 15

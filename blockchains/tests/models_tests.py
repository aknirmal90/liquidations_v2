from unittest.mock import patch

import pytest
from django.test import TestCase

from blockchains.models import ApproximateBlockTimestamp, Contract, Event, Network, Protocol
from utils.exceptions import ABINotFoundError, ConfigFileNotFoundError, EventABINotFoundError


@pytest.mark.django_db
class TestProtocolModel(TestCase):
    """Tests for the Protocol model."""

    def test_protocol_creation_and_properties(self) -> None:
        """Test protocol creation, defaults, and config/abi behavior with missing files."""
        protocol = Protocol.objects.create(name="test_protocol")

        # Test defaults
        assert protocol.name == "test_protocol"
        assert protocol.is_enabled is False
        assert protocol.config_path == "test_protocol/config.yaml"
        assert str(object=protocol) == "test_protocol"

        # Test missing files
        with pytest.raises(expected_exception=ConfigFileNotFoundError):
            _ = protocol.config

        with pytest.raises(expected_exception=ABINotFoundError):
            _ = protocol.evm_abi

    def test_get_evm_event_abi_behavior(self) -> None:
        """Test the get_evm_event_abi method's different scenarios."""
        protocol = Protocol.objects.create(name="test_protocol")

        mock_abi = [
            {"type": "event", "name": "test_event"},
            {"type": "event", "name": "other_event"}
        ]

        with patch('blockchains.models.Protocol.evm_abi', new=mock_abi):
            # Test successful event retrieval
            event_abi = protocol.get_evm_event_abi(name="test_event")
            assert event_abi == {"type": "event", "name": "test_event"}

            # Test missing event raises EventABINotFoundError
            with pytest.raises(expected_exception=EventABINotFoundError):
                protocol.get_evm_event_abi(name="non_existent_event")

    def test_get_protocol_by_name_caching(self) -> None:
        """Test the protocol caching functionality."""
        # Test empty name returns None
        assert Protocol.get_protocol_by_name(protocol_name="") is None

        # Test protocol retrieval and caching
        protocol = Protocol.objects.create(name="test_protocol")
        cached_protocol = Protocol.get_protocol_by_name(protocol_name="test_protocol")
        assert cached_protocol.name == protocol.name
        assert cached_protocol.is_enabled == protocol.is_enabled

        protocol.is_enabled = True
        protocol.save()
        cached_protocol = Protocol.get_protocol_by_name(protocol_name="test_protocol")
        assert cached_protocol.is_enabled == protocol.is_enabled


@pytest.mark.django_db
class TestNetworkModel(TestCase):
    def test_network_defaults(self) -> None:
        network = Network.objects.create(name="test_network")
        assert network.name == "test_network"
        assert network.rpc is None
        assert network.chain_id is None
        assert network.wss_infura is None
        assert network.wss_sequencer_oregon is None
        assert network.latest_block == 0

    def test_network_str(self) -> None:
        network = Network.objects.create(name="test_network")
        assert str(object=network) == "test_network"

    def test_get_network_by_name_caching(self) -> None:
        """Test the network caching functionality."""
        # Test empty name returns None
        assert Network.get_network_by_name(network_name="") is None

        # Test network retrieval and caching
        network = Network.objects.create(name="test_network", chain_id=1)
        cached_network = Network.get_network_by_name(network_name="test_network")
        assert cached_network.name == network.name
        assert cached_network.chain_id == network.chain_id

        # Test cache updates when model is updated
        network.chain_id = 2
        network.save()
        cached_network = Network.get_network_by_name(network_name="test_network")
        assert cached_network.chain_id == network.chain_id

    def test_network_properties(self) -> None:
        """Test network properties and configurations."""
        network = Network.objects.create(
            name="test_network",
            rpc="https://test.rpc",
            chain_id=1,
            wss_infura="wss://test.infura",
            wss_sequencer_oregon="wss://test.sequencer"
        )

        # Test all properties are set correctly
        assert network.rpc == "https://test.rpc"
        assert network.chain_id == 1
        assert network.wss_infura == "wss://test.infura"
        assert network.wss_sequencer_oregon == "wss://test.sequencer"

        # Test latest block updates
        network.latest_block = 1000
        network.save()
        assert network.latest_block == 1000

    def test_get_network_by_id(self) -> None:
        """Test retrieving network by chain ID."""
        # Test non-existent chain ID returns None
        with pytest.raises(expected_exception=Network.DoesNotExist):
            Network.get_network_by_id(id=999)

        # Create and test retrieval
        network = Network.objects.create(name="test_network", chain_id=1)
        retrieved_network = Network.get_network_by_id(id=network.id)
        assert retrieved_network is not None
        assert retrieved_network.name == network.name
        assert retrieved_network.chain_id == network.chain_id

        # Test cache updates when model is updated
        network.chain_id = 2
        network.save()
        cached_network = Network.get_network_by_id(id=network.id)
        assert cached_network.chain_id == network.chain_id


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

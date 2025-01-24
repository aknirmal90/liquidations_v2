
from unittest.mock import patch

import pytest

from blockchains.models import Event, Network, Protocol


@pytest.fixture(autouse=True)
def setup_celery(settings):
    settings.CELERY_TASK_ALWAYS_EAGER = True


@pytest.fixture
def mock_yaml_data():
    protocols_data = [
        {'name': 'aave', 'is_enabled': True},
    ]

    networks_data = [
        {
            'name': 'arbitrum',
            'chain_id': 1,
            'rpc': 'https://test.eth'
        }
    ]
    return protocols_data, networks_data


@pytest.fixture
def mock_config_data():
    return {
        'networks': [
            {
                'network': 'arbitrum',
                'contract_addresses': ['0x1234567890abcdef']
            }
        ],
        'events': [
            {
                'name': 'TestEvent',
                'model_class': 'TestModel'
            }
        ]
    }


@pytest.mark.django_db
class TestInitializeAppTask:

    @patch('blockchains.tasks.parse_yaml')
    @patch('blockchains.models.Protocol.get_evm_event_abi')
    def test_initialize_app_creates_protocol_and_network(
        self,
        mock_get_evm_event_abi,
        mock_parse_yaml,
        mock_yaml_data,
        mock_config_data,
    ) -> None:
        from blockchains.tasks import InitializeAppTask

        # Arrange
        protocols_data, networks_data = mock_yaml_data
        mock_parse_yaml.side_effect = [protocols_data, networks_data]

        mock_get_evm_event_abi.return_value = {
            'anonymous': False,
            'inputs': [],
            'name': 'TestEvent',
            'type': 'event'
        }

        with patch('blockchains.models.Protocol.config', return_value=mock_config_data):

            # Act
            InitializeAppTask.run()

            # Assert
            # Check if Protocol was created
            protocol = Protocol.objects.get(name='aave')
            assert protocol is not None
            assert protocol.is_enabled is True

            # Check if Network was created
            network = Network.objects.get(name='arbitrum')
            assert network is not None
            assert network.chain_id == 1
            assert network.rpc == 'https://test.eth'

            # Check if Event was created
            event = Event.objects.get(name='TestEvent')
            assert event is not None
            assert event.protocol.name == 'aave'
            assert event.network.name == 'arbitrum'
            assert event.model_class == 'TestModel'
            assert event.contract_addresses == ['0x1234567890abcdef']

    # @patch('blockchains.tasks.parse_yaml')
    # def test_initialize_app_updates_existing_protocol_and_network(self, mock_parse_yaml, mock_yaml_data):
    #     """
    #     Uses the existing 'protocol' and 'network' fixtures, then checks that
    #     the InitializeAppTask updates them properly based on the parsed YAML.
    #     """
    #     from blockchains.tasks import InitializeAppTask

    #     # Create initial protocol and network
    #     protocol = Protocol.objects.create(name='aave')
    #     network = Network.objects.create(name='arbitrum', chain_id=0, rpc_url="old.url")

    #     # Arrange
    #     protocols_data, networks_data = mock_yaml_data

    #     # Adjust the chain_id/rpc_url to something we can confirm changes
    #     networks_data[0]["chain_id"] = 42161
    #     networks_data[0]["rpc_url"] = "https://arbitrum-update.example"

    #     mock_parse_yaml.side_effect = [protocols_data, networks_data]

    #     with patch.object(Protocol, 'get_evm_event_abi', return_value={
    #         'anonymous': False,
    #         'inputs': [],
    #         'name': 'TestEvent',
    #         'type': 'event'
    #     }):
    #         # Act
    #         InitializeAppTask.delay()

    #         # Assert
    #         updated_protocol = Protocol.objects.get(pk=protocol.pk)
    #         assert updated_protocol.config == protocols_data[0]["config"]

    #         updated_network = Network.objects.get(pk=network.pk)
    #         assert updated_network.chain_id == networks_data[0]["chain_id"]
    #         assert updated_network.rpc_url == networks_data[0]["rpc_url"]

    @patch('blockchains.tasks.parse_yaml')
    def test_initialize_app_handles_empty_data(self, mock_parse_yaml):
        """
        Tests that the InitializeAppTask handles empty data gracefully.
        No networks, no protocols, no events will be created.
        """
        from blockchains.tasks import InitializeAppTask

        # Arrange
        mock_parse_yaml.side_effect = [[], []]

        # Act
        InitializeAppTask.delay()

        # Assert
        assert Protocol.objects.count() == 0
        assert Network.objects.count() == 0
        assert Event.objects.count() == 0

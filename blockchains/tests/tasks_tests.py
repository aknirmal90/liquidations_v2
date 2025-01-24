from unittest.mock import patch

import pytest

from blockchains.models import Event, Network, Protocol
from blockchains.tasks import (
    BackfillSynchronizeForEventTask,
    InitializeAppTask,
    ResetAppTask,
    StreamingSynchronizeForEventTask,
    SynchronizeForEventTask,
    UpdateBlockNumberTask,
    group_events_by_network,
    group_events_by_protocol,
)
from utils.constants import ACCEPTABLE_EVENT_BLOCK_LAG_DELAY


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


@pytest.fixture
def mock_event_abi():
    return {
        'anonymous': False,
        'inputs': [
            {
                'indexed': True,
                'name': 'param1',
                'type': 'address'
            },
            {
                'indexed': False,
                'name': 'param2',
                'type': 'uint256'
            }
        ],
        'name': 'TestEvent',
        'type': 'event'
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
        mock_event_abi,
    ) -> None:
        from blockchains.tasks import InitializeAppTask

        # Arrange
        protocols_data, networks_data = mock_yaml_data
        mock_parse_yaml.side_effect = [protocols_data, networks_data]
        mock_get_evm_event_abi.return_value = mock_event_abi

        with patch('blockchains.models.Protocol.config', new_callable=lambda: mock_config_data):
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

    @patch('blockchains.tasks.parse_yaml')
    @patch('blockchains.models.Protocol.get_evm_event_abi')
    def test_initialize_app_updates_existing_protocol_and_network(
        self,
        mock_get_evm_event_abi,
        mock_parse_yaml,
        mock_yaml_data,
        mock_config_data,
        mock_event_abi
    ) -> None:
        """
        Tests that running InitializeAppTask with modified config updates existing objects
        rather than creating new ones.
        """
        from blockchains.tasks import InitializeAppTask

        # Create initial protocol and network
        protocol = Protocol.objects.create(name='aave')
        network = Network.objects.create(name='arbitrum', chain_id=42160, rpc="old.url")

        # First run with initial config
        protocols_data, networks_data = mock_yaml_data
        mock_parse_yaml.side_effect = [protocols_data, networks_data]
        mock_get_evm_event_abi.return_value = mock_event_abi

        with patch('blockchains.models.Protocol.config', new_callable=lambda: mock_config_data):
            InitializeAppTask.run()

            # Store initial object counts
            initial_counts = {
                'protocol': Protocol.objects.count(),
                'network': Network.objects.count(),
                'event': Event.objects.count()
            }

            # Modify the mock data for second run
            modified_protocols_data = protocols_data.copy()
            modified_protocols_data[0]['is_enabled'] = False
            modified_networks_data = networks_data.copy()
            modified_networks_data[0]['rpc'] = "new.url"
            mock_parse_yaml.side_effect = [modified_protocols_data, modified_networks_data]

            # Modify config data to change event model class
            modified_config_data = mock_config_data.copy()
            modified_config_data['events'][0]['model_class'] = 'UpdatedTestModel'

            # Run task again with modified config
            with patch('blockchains.models.Protocol.config', new_callable=lambda: modified_config_data):
                InitializeAppTask.run()

                # Assert counts haven't changed (no new objects created)
                assert Protocol.objects.count() == initial_counts['protocol']
                assert Network.objects.count() == initial_counts['network']
                assert Event.objects.count() == initial_counts['event']

                # Assert objects were updated with new values
                updated_protocol = Protocol.objects.get(pk=protocol.pk)
                assert updated_protocol.is_enabled is False

                updated_network = Network.objects.get(pk=network.pk)
                assert updated_network.rpc == "new.url"

                # Verify event relationships and updated model class
                event = Event.objects.get(name='TestEvent')
                assert event is not None
                assert event.protocol == updated_protocol
                assert event.network == updated_network
                assert event.model_class == 'UpdatedTestModel'

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


@pytest.mark.django_db
class TestResetAppTask:

    def assert_model_counts(self, expected_count):
        assert Protocol.objects.count() == expected_count
        assert Network.objects.count() == expected_count
        assert Event.objects.count() == expected_count

    @patch('blockchains.tasks.parse_yaml')
    def test_reset_app_deletes_all_data(self, mock_parse_yaml, mock_config_data, mock_event_abi):
        """
        Tests that the ResetAppTask properly deletes all protocols, networks and events.
        """
        # Arrange
        mock_parse_yaml.side_effect = [
            [{'name': 'aave', 'is_enabled': True}],
            [{'name': 'arbitrum', 'chain_id': 1, 'rpc': 'https://test.eth'}]
        ]

        # Create initial data
        with patch('blockchains.models.Protocol.config', new_callable=lambda: mock_config_data), \
                patch('blockchains.models.Protocol.get_evm_event_abi', return_value=mock_event_abi):
            InitializeAppTask.run()

        # Verify initial data exists
        self.assert_model_counts(expected_count=1)

        # Act
        ResetAppTask.run()

        # Assert all data was deleted
        self.assert_model_counts(expected_count=0)

    @patch('blockchains.tasks.parse_yaml')
    def test_reset_app_handles_empty_database(self, mock_parse_yaml):
        """
        Tests that the ResetAppTask handles an empty database gracefully.
        """
        # Act
        ResetAppTask.run()

        # Assert no errors occurred and counts are 0
        self.assert_model_counts(expected_count=0)

    @patch('blockchains.tasks.parse_yaml')
    def test_reset_and_initialize_app(self, mock_parse_yaml, mock_config_data, mock_event_abi):
        """
        Tests that after a reset, the app can be properly reinitialized.
        """
        # Arrange
        mock_yaml_data = [
            [{'name': 'aave', 'is_enabled': True}],
            [{'name': 'arbitrum', 'chain_id': 1, 'rpc': 'https://test.eth'}]
        ]
        mock_parse_yaml.side_effect = mock_yaml_data

        # Create initial data
        with patch('blockchains.models.Protocol.config', new_callable=lambda: mock_config_data), \
                patch('blockchains.models.Protocol.get_evm_event_abi', return_value=mock_event_abi):
            InitializeAppTask.run()
        self.assert_model_counts(expected_count=1)

        # Reset
        ResetAppTask.run()
        self.assert_model_counts(expected_count=0)

        # Reinitialize
        mock_parse_yaml.side_effect = mock_yaml_data
        with patch('blockchains.models.Protocol.config', new_callable=lambda: mock_config_data), \
                patch('blockchains.models.Protocol.get_evm_event_abi', return_value=mock_event_abi):
            InitializeAppTask.run()

        # Assert data is restored
        self.assert_model_counts(expected_count=1)


@pytest.mark.django_db
class TestUpdateBlockNumberTask:
    def test_update_block_number_task(self):
        """
        Tests that UpdateBlockNumberTask correctly updates block numbers using real network data.
        """
        # Arrange
        network = Network.objects.create(
            name='arbitrum',
            chain_id=42161,
            rpc='https://arbitrum-mainnet.infura.io/v3/462361faca234b7f871c6c4a77ca51d0'
        )

        # Act
        UpdateBlockNumberTask.run()

        # Assert
        network.refresh_from_db()
        assert network.latest_block is not None
        assert network.latest_block > 0
        assert isinstance(network.latest_block, int)


@pytest.mark.django_db
class TestEventGrouping:
    def test_group_events_by_network(self, mock_config_data, mock_event_abi):
        """
        Tests that events are correctly grouped by network.
        """
        # Arrange
        protocol = Protocol.objects.create(name='test_protocol')
        network1 = Network.objects.create(name='network1')
        network2 = Network.objects.create(name='network2')

        mock_config = mock_config_data.copy()
        mock_config['networks'].append({
            'network': 'network2',
            'contract_addresses': ['0x5678']
        })

        with patch('blockchains.models.Protocol.config', new_callable=lambda: mock_config), \
                patch('blockchains.models.Protocol.get_evm_event_abi', return_value=mock_event_abi):
            events = [
                Event.objects.create(name='event1', network=network1, protocol=protocol, abi=mock_event_abi),
                Event.objects.create(name='event2', network=network1, protocol=protocol, abi=mock_event_abi),
                Event.objects.create(name='event3', network=network2, protocol=protocol, abi=mock_event_abi)
            ]

            # Act
            grouped = group_events_by_network(events=events)

            # Assert
            assert len(grouped) == 2
            assert len(grouped[network1]) == 2
            assert len(grouped[network2]) == 1
            assert events[0] in grouped[network1]
            assert events[1] in grouped[network1]
            assert events[2] in grouped[network2]

    def test_group_events_by_protocol(self, mock_config_data, mock_event_abi) -> None:
        """
        Tests that events are correctly grouped by protocol.
        """
        # Arrange
        protocol1 = Protocol.objects.create(name='protocol1')
        protocol2 = Protocol.objects.create(name='protocol2')
        network = Network.objects.create(name='network')

        with patch('blockchains.models.Protocol.config', new_callable=lambda: mock_config_data), \
                patch('blockchains.models.Protocol.get_evm_event_abi', return_value=mock_event_abi):
            events = [
                Event.objects.create(name='event1', network=network, protocol=protocol1, abi=mock_event_abi),
                Event.objects.create(name='event2', network=network, protocol=protocol1, abi=mock_event_abi),
                Event.objects.create(name='event3', network=network, protocol=protocol2, abi=mock_event_abi)
            ]

            # Act
            grouped = group_events_by_protocol(events=events)

            # Assert
            assert len(grouped) == 2
            assert len(grouped[protocol1]) == 2
            assert len(grouped[protocol2]) == 1
            assert events[0] in grouped[protocol1]
            assert events[1] in grouped[protocol1]
            assert events[2] in grouped[protocol2]


@pytest.mark.django_db
class TestStreamingSynchronizeForEventTask:
    def test_get_queryset_filters_correctly(self, mock_config_data, mock_event_abi):
        """Test that get_queryset returns only events within acceptable block lag"""
        # Arrange
        network = Network.objects.create(name='network', latest_block=ACCEPTABLE_EVENT_BLOCK_LAG_DELAY + 2)
        protocol = Protocol.objects.create(name='protocol')

        with patch('blockchains.models.Protocol.config', new_callable=lambda: mock_config_data), \
                patch('blockchains.models.Protocol.get_evm_event_abi', return_value=mock_event_abi):

            event1 = Event.objects.create(
                name='event1',
                network=network,
                protocol=protocol,
                abi=mock_event_abi,
                last_synced_block=ACCEPTABLE_EVENT_BLOCK_LAG_DELAY + 1,
                is_enabled=True
            )
            event2 = Event.objects.create(
                name='event2',
                network=network,
                protocol=protocol,
                abi=mock_event_abi,
                last_synced_block=0,
                is_enabled=True
            )

            task = StreamingSynchronizeForEventTask

            # Act
            result = task.get_queryset([event1.id, event2.id])

            # Assert
            assert event1 in result
            assert event2 not in result

            task = BackfillSynchronizeForEventTask

            # Act
            result = task.get_queryset([event1.id, event2.id])

            # Assert
            assert event1 not in result
            assert event2 in result


@pytest.mark.django_db
class TestSynchronizeForEventTask:
    def test_run_calls_both_tasks(self):
        """Test that run method calls both streaming and backfill tasks"""
        # Arrange
        event_ids = [1, 2, 3]

        with patch('blockchains.tasks.StreamingSynchronizeForEventTask.delay') as mock_streaming, \
                patch('blockchains.tasks.BackfillSynchronizeForEventTask.delay') as mock_backfill:

            task = SynchronizeForEventTask

            # Act
            task.run(event_ids)

            # Assert
            mock_streaming.assert_called_once_with(event_ids)
            mock_backfill.assert_called_once_with(event_ids)


# @pytest.mark.django_db
# class TestBaseSynchronizeTask:
#     def test_process_raw_event_dicts(self):
#         """Test that process_raw_event_dicts correctly decodes event data using real Aave event ABI"""
#         # Arrange
#         task = BaseSynchronizeTask()
#         event_abi = {
#             'anonymous': False,
#             'inputs': [
#                 {'indexed': True, 'name': 'reserve', 'type': 'address'},
#                 {'indexed': True, 'name': 'user', 'type': 'address'},
#                 {'indexed': False, 'name': 'onBehalfOf', 'type': 'address'},
#                 {'indexed': False, 'name': 'amount', 'type': 'uint256'},
#                 {'indexed': False, 'name': 'borrowRateMode', 'type': 'uint256'},
#                 {'indexed': False, 'name': 'borrowRate', 'type': 'uint256'},
#                 {'indexed': False, 'name': 'referral', 'type': 'uint16'}
#             ],
#             'name': 'Borrow',
#             'type': 'event'
#         }

#         raw_event_dicts = [{
#             'topics': [
#                 # Borrow event signature
#                 HexBytes('0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'),
#                 # reserve address (Arbitrum USDC)
#                 HexBytes('0x000000000000000000000000ff970a61a04b1ca14834a43f5de4533ebddb5cc8'),
#                 # user address
#                 HexBytes('0x000000000000000000000000b56c2f0b653b2e0b10c9b928c8580ac5df02c7c7')
#             ],
#             'data': HexBytes(
#                 '0x'
#                 + 'b56c2f0b653b2e0b10c9b928c8580ac5df02c7c7'  # onBehalfOf
#                 + '0000000000000000000000000000000000000000000000000de0b6b3a7640000'  # amount (1 ETH)
#                 + '0000000000000000000000000000000000000000000000000000000000000002'  # borrowRateMode
#                 + '0000000000000000000000000000000000000000000000000000000000000064'  # borrowRate
#                 + '0000000000000000000000000000000000000000000000000000000000000000'  # referral
#             ),
#             'blockNumber': 1234,
#             'transactionHash': HexBytes('b' * 64),
#             'logIndex': 0,
#             'address': '0x794a61358D6845594F94dc1DB02A252b5b4814aD'  # Aave Pool address
#         }]

#         event_abis = {
#             '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f': event_abi
#         }

#         # Act
#         result = task.process_raw_event_dicts(raw_event_dicts, event_abis)

#         # Assert
#         assert len(result) == 1
#         topic0 = '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'
#         assert topic0 in result
#         assert len(result[topic0]) == 1
#         event_data = result[topic0][0]
#         assert 'args' in event_data
#         assert 'blockNumber' in event_data
#         assert 'transactionHash' in event_data
#         assert 'logIndex' in event_data

    # def test_clean_event_logs_with_contract_addresses(self, mock_event_abi):
    #     """Test that clean_event_logs filters events by Aave contract addresses"""
    #     # Arrange
    #     task = BaseSynchronizeTask
    #     network = Network.objects.create(
    #         name='arbitrum',
    #         chain_id=42161,
    #         rpc='https://arbitrum-mainnet.infura.io/v3/462361faca234b7f871c6c4a77ca51d0'
    #     )
    #     protocol = Protocol.objects.create(name='aave')

    #     event = Event.objects.create(
    #         name='Mint',
    #         network=network,
    #         protocol=protocol,
    #         abi=mock_event_abi,
    #         contract_addresses=['0x794a61358D6845594F94dc1DB02A252b5b4814aD'],  # Aave Pool address
    #         last_synced_block=0
    #     )

    #     event_dicts = {
    #         event.topic_0: [
    #             {'address': '0x794a61358D6845594F94dc1DB02A252b5b4814aD', 'data': 'valid'},
    #             {'address': '0x5678', 'data': 'invalid'}
    #         ]
    #     }

    #     # Act
    #     result = task.clean_event_logs([event], event_dicts)

    #     # Assert
    #     assert len(result[event.topic_0]) == 1
    #     assert result[event.topic_0][0]['data'] == 'valid'

    # def test_clean_event_logs_without_contract_addresses(self, mock_event_abi):
    #     """Test that clean_event_logs keeps all events when no contract addresses specified"""
    #     # Arrange
    #     task = BaseSynchronizeTask
    #     network = Network.objects.create(
    #         name='arbitrum',
    #         chain_id=42161,
    #         rpc='https://arbitrum-mainnet.infura.io/v3/462361faca234b7f871c6c4a77ca51d0'
    #     )
    #     protocol = Protocol.objects.create(name='aave')

    #     event = Event.objects.create(
    #         name='Mint',
    #         network=network,
    #         protocol=protocol,
    #         abi=mock_event_abi,
    #         contract_addresses=[],
    #         last_synced_block=0
    #     )

    #     event_dicts = {
    #         event.topic_0: [
    #             {'address': '0x794a61358D6845594F94dc1DB02A252b5b4814aD', 'data': 'first'},
    #             {'address': '0x8145edddf43f50276641b55bd3ad95944510021e', 'data': 'second'}
    #         ]
    #     }

    #     # Act
    #     result = task.clean_event_logs([event], event_dicts)

    #     # Assert
    #     assert len(result[event.topic_0]) == 2

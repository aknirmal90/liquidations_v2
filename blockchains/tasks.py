import logging

from celery import Task

from blockchains.models import Network, Protocol
from liquidations_v2.celery_app import app
from utils.files import parse_yaml

logger = logging.getLogger(__name__)


class InitializeAppTask(Task):
    def run(self):
        logger.info("Starting InitializeAppTask")
        protocols = parse_yaml("protocols.yaml")
        networks = parse_yaml("networks.yaml")

        self.create_protocol_instances(protocols)
        self.create_network_instances(networks)
        logger.info("Completed InitializeAppTask")

    def create_protocol_instances(self, protocols):
        for protocol_data in protocols:
            Protocol.objects.create(**protocol_data)
            logger.info(f"Created Protocol instance: {protocol_data['name']}")

    def create_network_instances(self, networks):
        for network_data in networks:
            Network.objects.create(**network_data)
            logger.info(f"Created Network instance: {network_data['name']}")


InitializeAppTask = app.register_task(InitializeAppTask())

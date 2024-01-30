#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, Mapping, cast

import uuid
import requests
from destination_credal_custom.config import CredalConfig
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from logging import Logger, getLogger
from destination_credal_custom.config import CredalConfig
from destination_credal_custom.client import CredalClient
from destination_credal_custom.writer import CredalWriter
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type

logger = getLogger("airbyte")

class DestinationCredalCustom(Destination):
    def write(
        self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:

        """
        TODO
        Reads the input stream of messages, config, and catalog to write data to the destination.

        This method returns an iterable (typically a generator of AirbyteMessages via yield) containing state messages received
        in the input message stream. Outputting a state message means that every AirbyteRecordMessage which came before it has been
        successfully persisted to the destination. This is used to ensure fault tolerance in the case that a sync fails before fully completing,
        then the source is given the last state message output from this method as the starting point of the next sync.

        :param config: dict of JSON configuration matching the configuration declared in spec.json
        :param configured_catalog: The Configured Catalog describing the schema of the data being received and how it should be persisted in the
                                    destination
        :param input_messages: The stream of input messages received from the source
        :return: Iterable of AirbyteStateMessages wrapped in AirbyteMessage structs
        """
        # Generate a unique ID for this sync
        sync_id = uuid.uuid4()
        config = cast(CredalConfig, config)
        writer = CredalWriter(CredalClient(config, logger, configured_catalog, sync_id))
        logger.info(f"Starting sync {sync_id}")
        
        
        # Process records
        for message in input_messages:
            if message.type == Type.STATE:
                # Emitting a state message indicates that all records which came before it have been written to the destination.
                logger.info(f"State message received, checkpointing")
                yield message
            elif message.type == Type.RECORD and message.record is not None:
                writer.queue_airbyte_message(config, message.record)
            else:
                # ignore other message types for now
                continue

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the destination with the needed permissions
            e.g: if a provided API token or password can be used to connect and write to the destination.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this destination, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            config = cast(CredalConfig, config)
            deployment_url = config["deployment_url"]
            api_token = config["api_token"]
            url = f"{deployment_url}/api/health"
            headers = {"Authorization": f"Bearer {api_token}"}
            resp = requests.get(url, headers=headers)
            if resp.status_code == 200:
                return AirbyteConnectionStatus(status=Status.SUCCEEDED)
            else:
                return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(resp)}")
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
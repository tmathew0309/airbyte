#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import time
from collections.abc import Mapping
from typing import Any, List, Optional, Dict
from airbyte_cdk.models import AirbyteRecordMessage
from destination_credal_custom.config import CredalConfig
from langchain.utils import stringify_dict
import dpath.util


from destination_credal_custom.client import CredalClient
from logging import getLogger
import logging

STREAM_SEPERATOR = "__"

logger = getLogger("airbyte")

class CredalWriter:
    """
    Send invidual messages to Credal. This writer will eventually buffer messages before sending.
    """
    def __init__(self, client: CredalClient):
        self.client = client

    def queue_airbyte_message(self, config: CredalConfig, record: AirbyteRecordMessage) -> None:
        """Write now this just directly writes to Credal. Eventually we will buffer messages before sending.""" 
        self.client.write(airbyte_record=record.data, airbyte_stream_name=record.stream, collection_id=config.get("collection_id"))
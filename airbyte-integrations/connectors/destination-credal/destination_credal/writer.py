#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import time
from collections.abc import Mapping
from typing import Any, List, Optional, Dict
from airbyte_cdk.models import AirbyteRecordMessage
from destination_credal.config import CredalConfig
from langchain.utils import stringify_dict
import dpath.util


from destination_credal.client import CredalClient


STREAM_SEPERATOR = "__"

class CredalWriter:
    """
    Send invidual messages to Credal. This writer will eventually buffer messages before sending.
    """
    def __init__(self, client: CredalClient):
        self.client = client

    def queue_airbyte_message(self, config: CredalConfig, record: AirbyteRecordMessage) -> None:
        """Write now this just directly writes to Credal. Eventually we will buffer messages before sending."""
        document_name = record.data[config["document_name_field"]]
        document_id = str(record.data[config["document_id_field"]]) # These are sometimes ints so converting to string heres
        document_url = record.data[config["document_url_field"]]
        text_contents = stringify_dict(self._extract_fields_from_message(record, config["text_fields"]))
        custom_metadata = self._extract_fields_from_message(record, config["metadata_fields"])
        source_type = self._extract_source_type_from_stream(record.stream)
        self.client.write(document_name=document_name, document_contents=text_contents, custom_metadata=custom_metadata, source_type=source_type, document_id=document_id, document_url=document_url)


    def _extract_fields_from_message(self, record: AirbyteRecordMessage, fields: Optional[List[str]]) -> Dict[str, Any]:
        relevant_fields = {}
        if fields and len(fields) > 0:
            for field in fields:
                values = dpath.util.values(record.data, field, separator=".")
                if values and len(values) > 0:
                    relevant_fields[field] = values if len(values) > 1 else values[0]
        else:
            relevant_fields = record.data
        return relevant_fields

    def _extract_source_type_from_stream(self, stream_name: str) -> Dict[str, Any]:
        stream_source = stream_name.split(STREAM_SEPERATOR)[0]
        steam_source_to_source_type = {
            'zendesk': 'Zendesk',
            'salesforce': 'Salesforce',
        }
        return steam_source_to_source_type.get(stream_source, 'Manual Upload')
#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, Optional, Mapping
from airbyte_cdk import AirbyteLogger
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from logging import getLogger
import logging

import requests
from destination_credal_custom.config import CredalConfig


class CredalRateLimitError(Exception):
    pass

logger = getLogger("airbyte")
class CredalClient:
    def __init__(self, config: CredalConfig, logger: AirbyteLogger):
        self.deployment_url = config["deployment_url"]
        self.api_token = config["api_token"]
        self.uploader_email = config["uploader_email"]
        self.organization_id = config["organization_id"]

    @retry(
        retry=retry_if_exception_type(CredalRateLimitError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=90, max=240),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def write(self, airbyte_record: Any, airbyte_stream_name: str, collection_id: Optional[str]) -> requests.Response:
        """
        See Credal docs: https://docs.credal.ai/api-reference/document-catalog/upload-document-contents
        """
        request_body = {"airbyteRecord": airbyte_record, "uploadAsUserEmail": self.uploader_email, "collectionId": collection_id, "airbyteStreamName": airbyte_stream_name, "organizationId": self.organization_id}
        url = f"{self.deployment_url}/api/v0/airbyte/uploadAirbyteRecord"
        headers = {
            "Accept": "application/json",
            **self._get_auth_headers(),
        }

        response = requests.request(method="POST", url=url, headers=headers, json=request_body)

        if response.status_code != 200:
            try: 
                response_json = response.json()
            except:
                response_json = {}
            finally:
                logger.error(f"Request to {url} failed with: {response.status_code} {response_json}")
                if response.status_code == 429:
                    raise CredalRateLimitError(f"Request to {url} failed with: {response.status_code} {response_json}. Will potentially retry.")
                else:
                    raise Exception(f"Request to {url} failed with: {response.status_code} {response_json}")   
        return response

    def _get_auth_headers(self) -> Mapping[str, str]:
        return {"Authorization": f"Bearer {self.api_token}"}
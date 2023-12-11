#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, List, Mapping

import requests
from destination_credal.config import CredalConfig


class CredalClient:
    def __init__(self, config: CredalConfig):
        self.deployment_url = config["deployment_url"]
        self.api_token = config["api_token"]
        self.uploader_email = config["uploader_email"]

    def write(self, document_name: str, document_contents: str, custom_metadata: Mapping[str, Any], source_type: str) -> requests.Response:
        """
        See Credal docs: https://docs.credal.ai/api-reference/document-catalog/upload-document-contents
        """
        request_body = {"documentName": document_name, "documentContents": document_contents, "customMetadata": custom_metadata, "documentSourceType": source_type, "uploaderEmail": self.uploader_email}
        url = f"{self.deployment_url}/api/v0/catalog/uploadDocumentContents"
        headers = {
            "Accept": "application/json",
            **self._get_auth_headers(),
        }

        response = requests.request(method="POST", url=url, headers=headers, json=request_body)

        if response.status_code != 200:
            raise Exception(f"Request to {url} failed with: {response.status_code}: {response.json()}")
        return response

    def _get_auth_headers(self) -> Mapping[str, str]:
        return {"Authorization": f"Bearer {self.api_token}"}

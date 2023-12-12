#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import TypedDict, List, Optional

CredalConfig = TypedDict(
    "CredalConfig",
    {
        "deployment_url": str,
        "api_token": str,
        "uploader_email": str,
        "organization_id": str,
        "document_name_field": str,
        "text_fields": List[str],
        "metadata_fields": Optional[List[str]],
        "document_id_field": str,
        "document_url_field": Optional[str],
        "source_updated_timestamp_field": Optional[str],
        "collection_id": Optional[str],
    },
)
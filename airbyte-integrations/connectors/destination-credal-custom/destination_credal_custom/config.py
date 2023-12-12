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
        "collection_id": Optional[str],
    },
)
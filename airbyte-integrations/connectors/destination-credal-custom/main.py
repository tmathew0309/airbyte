#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from destination_credal_custom import DestinationCredalCustom

if __name__ == "__main__":
    DestinationCredalCustom().run(sys.argv[1:])

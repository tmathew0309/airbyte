#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from destination_credal import DestinationCredal

if __name__ == "__main__":
    DestinationCredal().run(sys.argv[1:])

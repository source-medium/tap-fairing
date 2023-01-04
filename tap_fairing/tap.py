"""fairing tap class."""
from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_fairing.streams import (
    ResponsesStream,
)

STREAM_TYPES = [
    ResponsesStream,
]


class Tapfairing(Tap):
    """fairing tap class."""

    name = "tap-fairing"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "secret_token",
            th.StringType,
            required=True,
            secret=True,
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            default="2010-01-01T00:00:00Z",
            description="The earliest record date to sync",
        ),
        th.Property(
            "page_size",
            th.IntegerType,
            default=100,
            description="The page size for each responses endpoint call",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    Tapfairing.cli()

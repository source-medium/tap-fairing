"""REST client handling, including fairingStream base class."""
from pathlib import Path
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIKeyAuthenticator

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class FairingStream(RESTStream):
    """fairing stream class."""

    url_base = "https://app.fairing.co/api"

    records_jsonpath = "$.data[*]"
    http_headers = {"Accept": "application/json"}

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object."""
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="Authorization",
            value=self.config.get("secret_token"),
            location="header",
        )

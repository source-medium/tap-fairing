"""Stream type classes for tap-fairing."""

from datetime import datetime, timezone

from typing import Any, Dict, Iterable, List, Optional, TypeVar

import dateutil.parser
import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.pagination import BaseAPIPaginator, JSONPathPaginator

from tap_fairing.client import FairingStream

_TToken = TypeVar("_TToken")


class ResponsesStream(FairingStream):
    """The responses from the fairing.co API."""

    name = "responses"
    path = "/responses"
    primary_keys = ["id"]
    replication_key = "id"
    is_sorted = True
    check_sorted = False
    schema = th.PropertiesList(
        th.Property(
            "id",
            th.StringType,
            required=True,
            description="The unique ID of the response.",
        ),
        th.Property("available_responses", th.ArrayType(th.StringType)),
        th.Property(
            "clarification_question",
            th.BooleanType,
            description="The question for this response was a clarification of a previous question if true.",
        ),
        th.Property(
            "coupon_amount",
            th.StringType,
            description="The discount amount applied to the order from the coupon.",
        ),
        th.Property(
            "coupon_code",
            th.StringType,
            description="The coupon code used on the order.",
        ),
        th.Property(
            "coupon_type",
            th.StringType,
            description="The coupon type used on the order. For Shopify orders, this will be either fixed or percentage.",
        ),
        th.Property(
            "customer_id",
            th.StringType,
            description="The ID of customer associated with the order for this response. For Shopify orders, this matches the customer's ID in Shopify.",
        ),
        th.Property(
            "customer_order_count",
            th.IntegerType,
            description="The total number of orders the customer had at the time they placed the order associated with this response.",
        ),
        th.Property(
            "email",
            th.StringType,
            description="The email address of the customer associated with the order for this response.",
        ),
        th.Property(
            "inserted_at",
            th.DateTimeType,
            description="ISO 8601 timestamp of the time the response was created.",
        ),
        th.Property(
            "landing_page_path",
            th.StringType,
            description="The first click landing page associated with the order for this response.",
        ),
        th.Property(
            "order_currency_code",
            th.StringType,
            description="The three-letter ISO 4217 currency code of the order.",
        ),
        th.Property(
            "order_id",
            th.StringType,
            description="The ID of the order associated with this response.",
        ),
        th.Property(
            "order_number",
            th.StringType,
            description="The order number of the order associated with this response.",
        ),
        th.Property(
            "order_platform",
            th.StringType,
            description="The platform that provided the order information. For Shopify, this will be shopify.",
        ),
        th.Property(
            "order_source",
            th.StringType,
            description="The source of the order. For Shopify, this will be shopify_checkout.",
        ),
        th.Property(
            "order_total",
            th.NumberType,
            description="The order total in the currency used for payment by the customer.",
        ),
        th.Property(
            "order_total_usd",
            th.NumberType,
            description="The order total in U.S. Dollars.",
        ),
        th.Property(
            "other",
            th.BooleanType,
            description="Indicates if the response was free-form text.",
        ),
        th.Property(
            "other_response",
            th.StringType,
            description="If the response was free-form text, the value of the text.",
        ),
        th.Property(
            "question",
            th.StringType,
            description="The text value of the question associated with this response.",
        ),
        th.Property(
            "question_id",
            th.IntegerType,
            description="The ID of the question associated with this response.",
        ),
        th.Property(
            "question_type",
            th.StringType,
            description="The type of the question associated with this response. One of single_response, multi_response, or open_ended.",
        ),
        th.Property(
            "referring_question",
            th.StringType,
            description="If the question associated with this response is a clarification question, the text value of the question immediately preceeding this one.",
        ),
        th.Property(
            "referring_question_id",
            th.IntegerType,
            description="If the question associated with this response is a clarification question, the ID of the question immediately preceeding this one.",
        ),
        th.Property(
            "referring_question_response",
            th.StringType,
            description="If the question associated with this response is a clarification question, the text value of the response provided to the question immediately preceeding this one.",
        ),
        th.Property(
            "referring_question_response_id",
            th.IntegerType,
            description="If the question associated with this response is a clarification question, the ID of the response provided to the question immediately preceeding this one.",
        ),
        th.Property(
            "referring_site",
            th.StringType,
            description="The referring site of the order associated with this response.",
        ),
        th.Property(
            "response",
            th.StringType,
            description="The text value of the response. This will be null if the response was free-form text.",
        ),
        th.Property(
            "response_id",
            th.IntegerType,
            description="The ID of the response if it was not free-form text.",
        ),
        th.Property(
            "response_position",
            th.IntegerType,
            description="The position of this response in the list of all responses to the associated question at the time it was presented to the customer. If the order of responses for a question is configured to be randomized, this value will not always be the same for the same response.",
        ),
        th.Property(
            "response_provided_at",
            th.DateTimeType,
            description="ISO 8601 timestamp of the time the response was provided by the customer.",
        ),
        th.Property(
            "submit_delta",
            th.IntegerType,
            description="The time, in milliseconds, bewtween when the question was displayed and the response was submitted.",
        ),
        th.Property(
            "updated_at",
            th.DateTimeType,
            description="ISO 8601 timestamp of the last time the response was updated.",
        ),
        th.Property(
            "utm_campaign",
            th.StringType,
            description="The UTM campaign of the order associated with this response.",
        ),
        th.Property(
            "utm_content",
            th.StringType,
            description="The UTM content of the order associated with this response.",
        ),
        th.Property(
            "utm_medium",
            th.StringType,
            description="The UTM medium of the order associated with this response.",
        ),
        th.Property(
            "utm_source",
            th.StringType,
            description="The UTM source of the order associated with this response.",
        ),
        th.Property(
            "utm_term",
            th.StringType,
            description="The UTM term of the order associated with this response.",
        ),
    ).to_dict()

    newest_id_jsonpath = "$.data[0].id"

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Get a fresh paginator for this API endpoint.

        Returns:
            A paginator instance.
        """
        return JSONPathPaginator(self.newest_id_jsonpath)

    def get_records(self, context: Optional[dict]) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        state = self.get_context_state(context)
        if state["starting_replication_value"] == self.config["start_date"]:
            # no state
            initial_url_params = self._find_oldest_page(
                context, state["starting_replication_value"]
            )
        else:
            # incremental run, start from latest id
            initial_url_params = {
                "before": state["starting_replication_value"],
                "limit": self.config["page_size"],
            }

        self.logger.info("Starting replication with params %s", initial_url_params)

        context = context or {}
        context["initial_url_params"] = initial_url_params

        for record in self.request_records(context):
            transformed_record = self.post_process(record, context)
            if transformed_record is None:
                # Record filtered out during post_process()
                continue
            yield transformed_record

    def _find_oldest_page(self, context, start_date) -> dict:
        """Finds the url params that produce that oldest partial page of results (after
        start_date) return context"""

        self.logger.info(
            f"No state found, looking for oldest results after start_date: {start_date}"
        )
        start_date = dateutil.parser.isoparse(start_date)
        params_for_start_date = self._check_start_date(context, start_date)
        if params_for_start_date:
            return params_for_start_date

        now = datetime.now(timezone.utc)
        return self._binary_search_for_oldest(context, now, now - start_date)

    def _prepare_search_request(self, context, until_dt):
        http_method = self.rest_method
        url: str = self.get_url(context)
        params: dict = {
            "until": until_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "limit": 100,
        }
        headers = self.http_headers
        return self.build_prepared_request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
        )

    def _make_search_request(self, context, until_dt):
        prepared_request = self._prepare_search_request(context, until_dt)
        resp = self._request(prepared_request, context)
        resp.raise_for_status()
        return resp

    def _check_start_date(self, context, start_date):
        """Check if there are results at the configured start_date. If so, find url params for
        that page"""
        resp = self._make_search_request(context, start_date)
        records = resp.json()["data"]
        if len(records) > 0:
            self.logger.warn(
                f"There are records available before start_date {start_date}. Consider removing that config if you wish to replicate all data."
            )
            return {"before": records[0]["id"], "limit": self.config["page_size"]}

        return False

    def _binary_search_for_oldest(self, context, interval_end, interval_delta):
        if interval_end > datetime.now(timezone.utc):
            raise RuntimeError(
                "Couldn't find any responses. Are you sure you've configured everything correctly?"
            )

        resp = self._make_search_request(context, interval_end)
        records = resp.json()["data"]
        if len(records) == 0:
            self.logger.debug(
                "No records at %s, searching more recent timestamp", interval_end
            )
            return self._binary_search_for_oldest(
                context, interval_end + (interval_delta / 2), interval_delta / 2
            )
        if len(records) == 100:
            self.logger.debug(
                "Full page of records at %s, searching older timestamp", interval_end
            )
            return self._binary_search_for_oldest(
                context, interval_end - (interval_delta / 2), interval_delta / 2
            )

        self.logger.debug(
            "Found a partial page of records at %s, returning first page", interval_end
        )
        if len(records) < self.config["page_size"]:
            return {
                "until": interval_end.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "limit": self.config["page_size"],
            }

        # yes, their API returns datetimes in a format they don't accept :facepalm:
        until_time = dateutil.parser.isoparse(
            records[-self.config["page_size"]]["inserted_at"]
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return {"until": until_time, "limit": self.config["page_size"]}

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[_TToken]
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        If paging is supported, developers may override with specific paging logic.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Dictionary of URL query parameters to use in the request.
        """
        if "initial_url_params" in context:
            params = context["initial_url_params"]
            del context["initial_url_params"]
            return params
        elif next_page_token:
            return {"before": next_page_token, "limit": self.config["page_size"]}
        else:
            # this should never happen, maybe raise?
            return {"limit": self.config["page_size"]}

    def post_process(self, row: Dict, context: Optional[dict] = None) -> Optional[dict]:
        """Fairing: transform the number types into floats for loading.

        As needed, append or transform raw data to match expected structure.

        Optional. This method gives developers an opportunity to "clean up" the results
        prior to returning records to the downstream tap - for instance: cleaning,
        renaming, or appending properties to the raw record result returned from the
        API.

        Developers may also return `None` from this method to filter out
        invalid or not-applicable records from the stream.

        Args:
            row: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            The resulting record dict, or `None` if the record should be excluded.
        """
        if "order_total" in row and row["order_total"]:
            row["order_total"] = float(row["order_total"])
        if "order_total_usd" in row and row["order_total_usd"]:
            row["order_total_usd"] = float(row["order_total_usd"])
        return row

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Fairing: Since the records are always returned in descending time order, we
        have to reverse the results in order to update the state correctly.

        Parse the response and return an iterator of result records.

        Args:
            response: A raw `requests.Response`_ object.

        Yields:
            One item for every item found in the response.

        .. _requests.Response:
            https://requests.readthedocs.io/en/latest/api/#requests.Response

        """
        data: List = response.json()["data"]
        data.reverse()
        yield from data

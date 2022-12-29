"""Stream type classes for tap-decentraland-crashbot."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
import requests

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_decentraland_crashbot.client import DCLCrashbotStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class IncidentsListStream(DCLCrashbotStream):
    name = "incidents"
    path = "/list"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("update_number", th.IntegerType),
        th.Property("modified_at", th.DateTimeType),
        th.Property("reported_at", th.DateTimeType),
        th.Property("closed_at", th.DateTimeType),
        th.Property("status", th.StringType),
        th.Property("severity", th.StringType),
        th.Property("title", th.StringType),
        th.Property("description", th.StringType),
        th.Property("point", th.StringType),
        th.Property("contact", th.StringType),
        th.Property("rca_link", th.StringType),
        th.Property("modified_by", th.StringType),
    ).to_dict()


    # Join open/closed arrays
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        data = response.json()

        for d in data['open']:
            yield d
        for d in data['closed']:
            yield d
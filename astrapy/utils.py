from typing import Any, Dict, Tuple, Optional
import requests
from requests.models import Response
import logging
import re

from astrapy import __version__
from astrapy.types import JSON_DICT
from astrapy.defaults import DEFAULT_TIMEOUT

logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG) # Apply if wishing to debug requests


class http_methods:
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


package_name = __name__.split(".")[0]


def log_request_response(r: Response, json_data: Optional[JSON_DICT]) -> None:
    logger.debug(f"Request URL: {r.url}")
    logger.debug(f"Request method: {r.request.method}")
    logger.debug(f"Request headers: {r.request.headers}")

    if json_data:
        logger.debug(f"Request payload: {json_data}")

    logger.debug(f"Response status code: {r.status_code}")
    logger.debug(f"Response headers: {r.headers}")
    logger.debug(f"Response content: {r.text}")


def make_request(
    base_url: str,
    auth_header: str,
    token: str,
    method: str = http_methods.POST,
    path: Optional[str] = None,
    json_data: Optional[JSON_DICT] = None,
    url_params: Optional[JSON_DICT] = None,
) -> Response:
    r = requests.request(
        method=method,
        url=f"{base_url}{path}",
        params=url_params,
        json=json_data,
        timeout=DEFAULT_TIMEOUT,
        headers={auth_header: token, "User-Agent": f"{package_name}/{__version__}"},
    )

    if logger.isEnabledFor(logging.DEBUG):
        log_request_response(r, json_data)

    return r


def make_payload(top_level: str, **kwargs: Dict[str, Any]) -> JSON_DICT:
    params = {}
    for key, value in kwargs.items():
        params[key] = value

    json_query: JSON_DICT = {top_level: {}}

    # Adding keys only if they're provided
    for key, value in params.items():
        if value is not None:
            json_query[top_level][key] = value

    return json_query


def parse_endpoint_url(url: str) -> Tuple[str, str, str]:
    # Regular expression pattern to match the given URL format
    pattern = r"https://(?P<db_id>[a-fA-F0-9\-]{36})-(?P<db_region>[a-zA-Z0-9\-]+)\.(?P<db_hostname>[a-zA-Z0-9\-\.]+\.com)"

    match = re.match(pattern, url)
    if match:
        return (
            match.group("db_id"),
            match.group("db_region"),
            match.group("db_hostname"),
        )
    else:
        raise ValueError("Invalid URL format")

# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations
from typing import Any, cast, Callable, Dict, Iterable, List, Optional

from astrapy.defaults import (
    DEFAULT_AUTH_HEADER,
    DEFAULT_KEYSPACE_NAME,
    DEFAULT_BASE_PATH,
)
from astrapy.types import JSON_DICT
from astrapy.utils import make_payload, make_request, http_methods, parse_endpoint_url

import logging
import json

logger = logging.getLogger(__name__)


class AstraDBCollection:
    def __init__(
        self,
        collection_name: str,
        astra_db: Optional[AstraDB] = None,
        token: Optional[str] = None,
        api_endpoint: Optional[str] = None,
        namespace: Optional[str] = None,
    ):
        if astra_db is None:
            if token is None or api_endpoint is None:
                raise AssertionError("Must provide token and api_endpoint")

            astra_db = AstraDB(
                token=token, api_endpoint=api_endpoint, namespace=namespace
            )

        self.astra_db = astra_db
        self.collection_name = collection_name
        self.base_path = f"{self.astra_db.base_path}/{collection_name}"

    def _request(self, method: str, path: str, json_data: Optional[JSON_DICT] = None, url_params: Optional[JSON_DICT] = None, skip_error_check: bool= False) -> JSON_DICT:
        response = make_request(
            base_url=self.astra_db.base_url,
            auth_header=DEFAULT_AUTH_HEADER,
            token=self.astra_db.token,
            method=method,
            path=path,
            json_data=json_data,
            url_params=url_params,
        )
        responsebody = cast(JSON_DICT, response.json())

        if not skip_error_check and "errors" in responsebody:
            raise ValueError(json.dumps(responsebody["errors"]))
        else:
            return responsebody

    def _get(self, path: Optional[str] = None, options: Optional[JSON_DICT] = None) -> JSON_DICT:
        full_path = f"{self.base_path}/{path}" if path else self.base_path
        response = self._request(
            method=http_methods.GET, path=full_path, url_params=options
        )
        if isinstance(response, dict):
            return response
        return None

    def _post(self, path: Optional[str] = None, document: Optional[JSON_DICT] = None) -> JSON_DICT:
        response = self._request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=document
        )
        return response

    def _put(self, path: Optional[str] = None, document: Optional[JSON_DICT] = None) -> JSON_DICT:
        response = self._request(
            method=http_methods.PUT, path=f"{self.base_path}", json_data=document
        )
        return response

    def get(self, path: Optional[str] = None) -> JSON_DICT:
        return self._get(path=path)

    def find(self, filter: Optional[JSON_DICT] = None, projection: Optional[JSON_DICT] = None, sort: Optional[JSON_DICT] ={}, options: Optional[JSON_DICT] = None) -> JSON_DICT:
        json_query = make_payload(
            top_level="find",
            filter=filter,
            projection=projection,
            options=options,
            sort=sort,
        )

        response = self._post(
            document=json_query,
        )

        return response

    @staticmethod
    def paginate(*, request_method: Callable[..., JSON_DICT], options: Optional[JSON_DICT], **kwargs: Any) -> Iterable[JSON_DICT]:
        _options = options or {}
        response0 = request_method(options=_options, **kwargs)
        next_page_state = response0["data"]["nextPageState"]
        options0 = _options
        for document in response0["data"]["documents"]:
            yield document
        while next_page_state is not None:
            options1 = {**options0, **{"pagingState": next_page_state}}
            response1 = request_method(options=options1, **kwargs)
            for document in response1["data"]["documents"]:
                yield document
            next_page_state = response1["data"]["nextPageState"]

    def paginated_find(self, filter: Optional[JSON_DICT] = None, projection: Optional[JSON_DICT] = None, sort: Optional[JSON_DICT] = None, options: Optional[JSON_DICT] = None) -> Iterable[JSON_DICT]:
        return self.paginate(
            request_method=self.find,
            filter=filter,
            projection=projection,
            sort=sort,
            options=options,
        )

    def pop(self, filter: Optional[JSON_DICT], update: Optional[JSON_DICT], options: Optional[JSON_DICT]) -> JSON_DICT:
        json_query = make_payload(
            top_level="findOneAndUpdate", filter=filter, update=update, options=options
        )

        response = self._request(
            method=http_methods.POST,
            path=self.base_path,
            json_data=json_query,
        )

        return response

    def push(self, filter: Optional[JSON_DICT], update: Optional[JSON_DICT], options: Optional[JSON_DICT]) -> JSON_DICT:
        json_query = make_payload(
            top_level="findOneAndUpdate", filter=filter, update=update, options=options
        )

        response = self._request(
            method=http_methods.POST,
            path=self.base_path,
            json_data=json_query,
        )

        return response

    def find_one_and_replace(
        self, sort: JSON_DICT = {}, filter: Optional[JSON_DICT] = None, replacement: Optional[JSON_DICT] = None, options: Optional[JSON_DICT] = None
    ) -> JSON_DICT:
        json_query = make_payload(
            top_level="findOneAndReplace",
            filter=filter,
            replacement=replacement,
            options=options,
            sort=sort,
        )

        response = self._request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=json_query
        )

        return response

    def find_one_and_update(self, sort: Optional[JSON_DICT] = {}, update: Optional[JSON_DICT] = None, filter: Optional[JSON_DICT] = None, options: Optional[JSON_DICT] = None) -> JSON_DICT:
        json_query = make_payload(
            top_level="findOneAndUpdate",
            filter=filter,
            update=update,
            options=options,
            sort=sort,
        )

        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )

        return response

    def find_one(self, filter: Optional[JSON_DICT] = {}, projection: Optional[JSON_DICT] = {}, sort: Optional[JSON_DICT] = {}, options: Optional[JSON_DICT] = {}) -> JSON_DICT:
        json_query = make_payload(
            top_level="findOne",
            filter=filter,
            projection=projection,
            options=options,
            sort=sort,
        )

        response = self._post(
            document=json_query,
        )

        return response

    def insert_one(self, document: JSON_DICT) -> JSON_DICT:
        json_query = make_payload(top_level="insertOne", document=document)

        response = self._request(
            method=http_methods.POST, path=self.base_path, json_data=json_query
        )

        return response

    def insert_many(self, documents: Iterable[JSON_DICT], options: Optional[JSON_DICT] = None, partial_failures_allowed: bool = False) -> JSON_DICT:
        _documents = list(documents)
        json_query = make_payload(
            top_level="insertMany", documents=_documents, options=options
        )

        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
            skip_error_check=partial_failures_allowed,
        )

        return response

    def update_one(self, filter: JSON_DICT, update: JSON_DICT) -> JSON_DICT:
        json_query = make_payload(top_level="updateOne", filter=filter, update=update)

        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data=json_query,
        )

        return response

    def replace(self, path: str, document: JSON_DICT) -> JSON_DICT:
        return self._put(path=path, document=document)

    def delete(self, id: str) -> JSON_DICT:
        json_query = {
            "deleteOne": {
                "filter": {"_id": id},
            }
        }

        response = self._request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=json_query
        )

        return response

    def delete_subdocument(self, id: str, subdoc: str) -> JSON_DICT:
        json_query = {
            "findOneAndUpdate": {
                "filter": {"_id": id},
                "update": {"$unset": {subdoc: ""}},
            }
        }

        response = self._request(
            method=http_methods.POST, path=f"{self.base_path}", json_data=json_query
        )

        return response

    def upsert(self, document: JSON_DICT) -> str:
        """
        Emulate an upsert operation for a single document,
        whereby a document is inserted if its _id is new, or completely
        replaces and existing one if that _id is already saved in the collection.
        Returns: the _id of the inserted document.
        """
        # Attempt to insert the given document
        result = self.insert_one(document)

        # Check if we hit an error
        if (
            "errors" in result
            and "errorCode" in result["errors"][0]
            and result["errors"][0]["errorCode"] == "DOCUMENT_ALREADY_EXISTS"
        ):
            # Now we attempt to update
            result = self.find_one_and_replace(
                filter={"_id": document["_id"]},
                replacement=document,
            )
            upserted_id = cast(str, result["data"]["document"]["_id"])
        else:
            upserted_id = cast(str, result["status"]["insertedIds"][0])

        return upserted_id


class AstraDB:
    def __init__(
        self,
        token: Optional[str] = None,
        api_endpoint: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> None:
        if token is None or api_endpoint is None:
            raise AssertionError("Must provide token and api_endpoint")

        if namespace is None:
            logger.info(
                f"ASTRA_DB_KEYSPACE is not set. Defaulting to '{DEFAULT_KEYSPACE_NAME}'"
            )
            namespace = DEFAULT_KEYSPACE_NAME

        # Store the initial parameters
        self.token = token
        (
            self.database_id,
            self.database_region,
            self.database_domain,
        ) = parse_endpoint_url(api_endpoint)

        # Set the Base URL for the API calls
        self.base_url = (
            f"https://{self.database_id}-{self.database_region}.{self.database_domain}"
        )
        self.base_path = f"{DEFAULT_BASE_PATH}/{namespace}"

        # Set the namespace parameter
        self.namespace = namespace

    def _request(self, method: str, path: str, json_data: Optional[JSON_DICT], skip_error_check: bool = False) -> JSON_DICT:
        response = make_request(
            base_url=self.base_url,
            auth_header=DEFAULT_AUTH_HEADER,
            token=self.token,
            method=method,
            path=path,
            json_data=json_data,
        )
        responsebody = cast(JSON_DICT, response.json())

        if not skip_error_check and "errors" in responsebody:
            raise ValueError(json.dumps(responsebody["errors"]))
        else:
            return responsebody

    def collection(self, collection_name: str) -> AstraDBCollection:
        return AstraDBCollection(collection_name=collection_name, astra_db=self)

    def get_collections(self) -> JSON_DICT:
        response = self._request(
            method=http_methods.POST,
            path=self.base_path,
            json_data={"findCollections": {}},
        )

        return response

    def create_collection(
        self, options: Optional[JSON_DICT] = None, dimension: Optional[int] = None, metric: str = "", collection_name: str = ""
    ) -> JSON_DICT:
        # Initialize options if not passed
        if not options:
            options = {"vector": {}}
        elif "vector" not in options:
            options["vector"] = {}

        # Now check the remaining parameters - dimension
        if dimension:
            if "dimension" not in options["vector"]:
                options["vector"]["dimension"] = dimension
            else:
                raise ValueError(
                    "dimension parameter provided both in options and as function parameter."
                )

        # Check the metric parameter
        if metric:
            if "metric" not in options["vector"]:
                options["vector"]["metric"] = metric
            else:
                raise ValueError(
                    "metric parameter provided both in options as function parameter."
                )

        # Build the final json payload
        jsondata = {"name": collection_name, "options": options}

        # Make the request to the endpoitn
        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"createCollection": jsondata},
        )

        return response

    def delete_collection(self, collection_name: str = "") -> JSON_DICT:
        response = self._request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"deleteCollection": {"name": collection_name}},
        )

        return response

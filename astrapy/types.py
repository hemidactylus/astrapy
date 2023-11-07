from typing import Any, Dict

# A "dict from parsing a JSON" type.
#   Note this is not covering really all JSON parses (e.g. 'null' is valid JSON)
JSON_DICT = Dict[str, Any]

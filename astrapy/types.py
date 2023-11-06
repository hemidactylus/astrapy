from typing import Any, Dict

# A "dict from parsing a JSON" type.
#   Formally distinct from e.g. how one would type kwargs
#   and also not covering really all JSON parses (e.g. 'null' is valid JSON)
JSON_DICT = Dict[str, Any]

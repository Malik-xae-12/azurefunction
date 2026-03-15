from typing import Any


def success_response(data: Any) -> dict:
    return {"success": True, "data": data}

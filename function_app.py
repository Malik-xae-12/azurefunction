"""Azure Functions v2 entry point – wraps the existing FastAPI app via ASGI."""

import azure.functions as func

from app.app_factory import create_app

fastapi_app = create_app()

app = func.AsgiFunctionApp(app=fastapi_app, http_auth_level=func.AuthLevel.ANONYMOUS)

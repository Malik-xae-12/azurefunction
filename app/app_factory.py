from fastapi import FastAPI

from app.db.base import Base
from app.db.session import engine
from app.db import models  # noqa: F401
from app.modules.hubspot.router import router as hubspot_router


def create_app() -> FastAPI:
    app = FastAPI(title="HubSpot Loader")

    @app.on_event("startup")
    def on_startup() -> None:
        Base.metadata.create_all(bind=engine)

    app.include_router(hubspot_router)
    return app

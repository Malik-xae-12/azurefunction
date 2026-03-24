"""FastAPI application factory."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.db.base import Base
from app.db.session import engine
from app.db import models  
from app.modules.hubspot.router import router as hubspot_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create all tables on startup (idempotent — safe to run every time)
    # In production, prefer Alembic migrations over create_all.
    logger.info("Running Base.metadata.create_all ...")
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables ready.")
    yield
    logger.info("Application shutting down.")


def create_app() -> FastAPI:
    app = FastAPI(
        title="HubSpot Sync Service",
        version="1.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],   # tighten to your frontend domain in production
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

    app.include_router(hubspot_router)

    @app.get("/health")
    async def health():
        return {"status": "healthy"}

    return app
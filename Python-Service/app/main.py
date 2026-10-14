import asyncio
import sys
import ssl
import os
from contextlib import asynccontextmanager

# =========================================================
# SSL VERIFICATION (opt-in bypass only, never default)
# =========================================================
if os.getenv("DISABLE_SSL_VERIFY", "false").lower() in ("true", "1", "yes"):
    if os.getenv("ENVIRONMENT", "development").lower() not in ("local", "development", "dev"):
        raise RuntimeError(
            "DISABLE_SSL_VERIFY is set but ENVIRONMENT is not local/development. "
            "Refusing to disable TLS verification outside local dev."
        )
    try:
        ssl._create_default_https_context = ssl._create_unverified_context
    except AttributeError:
        pass

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.validation_routes import router as validation_router
from app.api.migration_routes import router as migration_router
from app.api.auth_routes import router as auth_router
from app.api.crm_routes import router as crm_router
from app.api.metadata_routes import router as metadata_router
from app.api.migration_history import router as migration_history
from app.api.file_migration_routes import router as file_migration_router
from app.services.staging_cleanup_service import run_staging_cleanup_loop
from app.api.file_migration_routes import router as file_migration_router
from app.api.run_routes import router as run_router


# =========================================================
# LIFESPAN (background staging-DB cleanup)
# =========================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    cleanup_task = asyncio.create_task(run_staging_cleanup_loop())
    yield
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="Migration Engine", lifespan=lifespan)

# =========================================================
# CORS
# =========================================================
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv("ALLOWED_ORIGINS", "http://localhost:4200").split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(metadata_router)
app.include_router(migration_router)
app.include_router(migration_history)
app.include_router(file_migration_router)
app.include_router(auth_router, prefix="/api/auth", tags=["Auth"])
app.include_router(crm_router, prefix="/api/crm", tags=["CRM Connections"])
app.include_router(validation_router)
app.include_router(file_migration_router)
app.include_router(run_router)


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=os.getenv("ENVIRONMENT", "development").lower() in ("local", "development", "dev"),
        ws_max_size=64 * 1024 * 1024,  # 64MB, up from 16MB default — large CSV migrate payloads over /ws/migrate need this
    )
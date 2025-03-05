import uvicorn
from fastapi import FastAPI

from streaq.web.htmx import router


def build_app() -> FastAPI:
    app = FastAPI(title="streaQ", docs_url=None, redoc_url=None)
    app.include_router(router)
    return app


def run_app(host: str, port: int):
    app = build_app()
    uvicorn.run(app, host=host, port=port)

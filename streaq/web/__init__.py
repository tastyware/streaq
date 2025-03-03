import uvicorn
from fastapi import FastAPI

from streaq.web.htmx import router


def run_app(host: str, port: int):
    app = FastAPI(title="streaQ", redoc_url=None)
    app.include_router(router)
    uvicorn.run(app, host=host, port=port)

from typing import Annotated
from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse

from streaq.ui import get_worker
from streaq.worker import Worker


router = APIRouter(prefix="/queues")


@router.get("/", response_class=HTMLResponse)
async def get_queues(worker: Annotated[Worker, Depends(get_worker)]):
    return """<div>Hello</div>"""

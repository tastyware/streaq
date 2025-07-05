from pathlib import Path
from typing import Any, AsyncGenerator

from fastapi import HTTPException, status
from fastapi.templating import Jinja2Templates

from streaq import Worker

BASE_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(BASE_DIR))


async def get_worker() -> AsyncGenerator[Worker[Any], None]:
    raise HTTPException(
        status_code=status.HTTP_412_PRECONDITION_FAILED,
        detail="get_worker dependency not implemented!",
    )

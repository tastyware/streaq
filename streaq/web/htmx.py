import os
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from redis.asyncio import Redis

# Use absolute path based on the location of this file
BASE_DIR = Path(__file__).resolve().parent
router = APIRouter(tags=["streaq"])
templates = Jinja2Templates(directory=BASE_DIR / "templates")


async def get_redis() -> Redis:
    url = os.getenv("REDIS_URL")
    if url is None:
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail="$REDIS_URL environment variable is missing!",
        )
    return Redis.from_url(url, decode_responses=True)


@router.get("/")
async def get_root(
    request: Request, redis: Annotated[Redis, Depends(get_redis)]
) -> HTMLResponse:
    return templates.TemplateResponse(request, "index.html")

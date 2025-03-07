import os
import re
from datetime import datetime
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from redis.asyncio import Redis

from streaq.constants import (
    REDIS_GROUP,
    REDIS_PREFIX,
    REDIS_QUEUE,
    REDIS_STREAM,
    REDIS_TIMEOUT,
)

# Use absolute path based on the location of this file
BASE_DIR = Path(__file__).resolve().parent
router = APIRouter(tags=["streaq"])
templates = Jinja2Templates(directory=BASE_DIR / "templates")
pattern = re.compile(f"{REDIS_PREFIX}(.+?){REDIS_STREAM}")


async def get_redis() -> Redis:
    url = os.getenv("REDIS_URL")
    if url is None:
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail="$REDIS_URL environment variable is missing!",
        )
    if url.startswith("rediss"):
        url += "?ssl_cert_reqs=none"
    return Redis.from_url(url, decode_responses=True)


@router.get("/")
async def get_root(
    request: Request, redis: Annotated[Redis, Depends(get_redis)]
) -> HTMLResponse:
    queue_names = [
        re.match(pattern, q).group(1)  # type: ignore
        async for q in redis.scan_iter(match=f"{REDIS_PREFIX}*{REDIS_STREAM}", count=10)
    ]
    queues = []
    for name in queue_names:
        stream_key = REDIS_PREFIX + name + REDIS_STREAM
        async with redis.pipeline(transaction=False) as pipe:
            pipe.xlen(stream_key)
            pipe.zcard(REDIS_PREFIX + name + REDIS_QUEUE)
            pipe.zcard(REDIS_PREFIX + name + REDIS_TIMEOUT)
            pipe.xinfo_consumers(stream_key, REDIS_GROUP)
            enqueued, scheduled, running, consumers = await pipe.execute()
        queues.append(
            {
                "name": name,
                "active": running,
                "enqueued": enqueued,
                "scheduled": scheduled,
                "workers": len(consumers),
            }
        )
    return templates.TemplateResponse(request, "index.html", context={"queues": queues})


@router.get("/queues/{name}")
async def get_queue(
    request: Request, redis: Annotated[Redis, Depends(get_redis)], name: str
) -> HTMLResponse:
    async with redis.pipeline(transaction=False) as pipe:
        pipe.xread({REDIS_PREFIX + name + REDIS_STREAM: "0-0"}, count=50)
        pipe.zrange(REDIS_PREFIX + name + REDIS_QUEUE, 0, -1, withscores=True)
        enqueued, deferred = await pipe.execute()
    # we just want the id and timestamp
    enqueued_dict = (
        {
            data["task_id"]: datetime.fromtimestamp(int(data["score"]) / 1000)
            for _, data in enqueued[0][1]
        }
        if enqueued
        else {}
    )
    deferred_dict = {id: datetime.fromtimestamp(ts / 1000) for id, ts in deferred}
    return templates.TemplateResponse(
        request,
        "queue.html",
        context={"deferred": deferred_dict, "enqueued": enqueued_dict},
    )

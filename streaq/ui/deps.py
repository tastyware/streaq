from typing import AsyncGenerator

from fastapi import HTTPException, status

from streaq import Worker


async def get_worker() -> AsyncGenerator[Worker, None]:
    raise HTTPException(
        status_code=status.HTTP_412_PRECONDITION_FAILED,
        detail="get_worker dependency not implemented!",
    )

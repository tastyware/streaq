from datetime import datetime
from typing import Any

from async_lru import alru_cache
from fastapi import Request
from pydantic import BaseModel

from streaq import TaskStatus, Worker
from streaq.constants import REDIS_RESULT, REDIS_RUNNING, REDIS_TASK


class TaskData(BaseModel):
    color: str
    text_color: str
    enqueue_time: str
    task_id: str
    status: TaskStatus
    fn_name: str
    sort_time: datetime
    get_url: str


@alru_cache(ttl=1)
async def get_context(request: Request, worker: Worker[Any]) -> dict[str, Any]:
    pipe = await worker.redis.pipeline(transaction=False)
    for priority in worker.priorities:
        await pipe.zrange(worker._queue_key + priority, 0, -1)  # type: ignore
    await pipe.xread(
        {worker._stream_key + p: "0-0" for p in worker.priorities},  # type: ignore
        count=1000,
    )
    await pipe.keys(worker._prefix + REDIS_RESULT + "*")  # type: ignore
    await pipe.keys(worker._prefix + REDIS_RUNNING + "*")  # type: ignore
    await pipe.keys(worker._prefix + REDIS_TASK + "*")  # type: ignore
    res = await pipe.execute()
    stream: set[str] = (
        set(t.field_values["task_id"] for v in res[-4].values() for t in v)
        if res[-4]
        else set()
    )
    queue: set[str] = set()
    for r in res[: len(worker.priorities)]:
        queue |= set(r)
    results = set(r.split(":")[-1] for r in res[-3])
    running = set(r.split(":")[-1] for r in res[-2])
    tasks: list[TaskData] = []
    to_fetch: list[str] = list(res[-1] | res[-3])
    serialized = await worker.redis.mget(to_fetch) if to_fetch else ()  # type: ignore
    for i, entry in enumerate(serialized):
        td = worker.deserialize(entry)
        task_id = to_fetch[i].split(":")[-1]
        if task_id in results:
            status = TaskStatus.DONE
            color = "success"
            text_color = "light"
        elif task_id in running:
            status = TaskStatus.RUNNING
            color = "warning"
            text_color = "dark"
        elif task_id in queue:
            status = TaskStatus.SCHEDULED
            color = "secondary"
            text_color = "light"
        else:
            status = TaskStatus.QUEUED
            color = "info"
            text_color = "dark"
        ts = td.get("et") or td.get("t") or 0
        dt = datetime.fromtimestamp(ts / 1000, tz=worker.tz)
        tasks.append(
            TaskData(
                color=color,
                text_color=text_color,
                enqueue_time=dt.strftime("%Y-%m-%d %H:%M:%S"),
                status=status,
                task_id=task_id,
                fn_name=td["f"],
                sort_time=dt,
                get_url=request.url_for("get_task", task_id=task_id).path,
            )
        )
    tasks.sort(key=lambda td: td.sort_time)
    return {
        "running": len(running),
        "queued": len(stream) - len(running),
        "scheduled": len(queue),
        "finished": len(results),
        "functions": list(worker.registry.keys()),
        "tasks": tasks,
        "title": worker.queue_name,
        "tasks_filter_url": request.url_for("filter_tasks").path,
    }

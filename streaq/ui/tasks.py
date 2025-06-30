from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from streaq import TaskPriority, TaskStatus, Worker
from streaq.constants import REDIS_RESULT, REDIS_RUNNING, REDIS_TASK
from streaq.ui.deps import get_worker, templates

router = APIRouter()


class TaskData(BaseModel):
    color: str
    text_color: str
    enqueue_time: str
    task_id: str
    status: TaskStatus
    fn_name: str
    sort_time: datetime


@router.get("/queue", response_class=HTMLResponse)
async def get_tasks(
    request: Request, worker: Annotated[Worker[Any], Depends(get_worker)]
) -> Any:
    pipe = await worker.redis.pipeline(transaction=False)
    await pipe.xread(
        {worker.stream_key + p.value: "0-0" for p in TaskPriority},
        count=100,
    )
    await pipe.zrange(worker.queue_key, 0, -1)
    await pipe.keys(worker._prefix + REDIS_RESULT + "*")  # type: ignore
    await pipe.keys(worker._prefix + REDIS_RUNNING + "*")  # type: ignore
    await pipe.keys(worker._prefix + REDIS_TASK + "*")  # type: ignore
    _stream, _queue, _results, _running, _data = await pipe.execute()
    stream: set[str] = (
        set(t.field_values["task_id"] for v in _stream.values() for t in v)
        if _stream
        else set()
    )
    queue = set(_queue)
    results = set(r.split(":")[-1] for r in _results)
    running = set(r.split(":")[-1] for r in _running)
    tasks: list[TaskData] = []
    to_fetch: list[str] = list(_data | _results)
    serialized = await worker.redis.mget(to_fetch)  # type: ignore
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
        dt = datetime.fromtimestamp((td.get("et") or td["t"]) / 1000, tz=worker.tz)
        tasks.append(
            TaskData(
                color=color,
                text_color=text_color,
                enqueue_time=dt.strftime("%Y-%m-%d %H:%M:%S"),
                status=status,
                task_id=task_id,
                fn_name=td["f"],
                sort_time=dt,
            )
        )
    tasks.sort(key=lambda td: td.sort_time)
    return templates.TemplateResponse(
        request,
        "queue.j2",
        context={
            "running": len(running),
            "queued": len(stream) - len(running),
            "scheduled": len(queue),
            "finished": len(results),
            "functions": list(worker.registry.keys()),
            "tasks": tasks,
            "title": worker.queue_name,
        },
    )


@router.post("/queue", response_class=HTMLResponse)
async def filter_tasks(
    request: Request,
    worker: Annotated[Worker[Any], Depends(get_worker)],
    functions: Annotated[list[str] | None, Form()] = None,
    statuses: Annotated[list[TaskStatus] | None, Form()] = None,
) -> Any:
    pipe = await worker.redis.pipeline(transaction=False)
    await pipe.zrange(worker.queue_key, 0, -1)
    await pipe.keys(worker._prefix + REDIS_RESULT + "*")  # type: ignore
    await pipe.keys(worker._prefix + REDIS_RUNNING + "*")  # type: ignore
    await pipe.keys(worker._prefix + REDIS_TASK + "*")  # type: ignore
    _queue, _results, _running, _data = await pipe.execute()
    queue = set(_queue)
    results = set(r.split(":")[-1] for r in _results)
    running = set(r.split(":")[-1] for r in _running)
    tasks: list[TaskData] = []
    to_fetch: list[str] = list(_data | _results)
    serialized = await worker.redis.mget(to_fetch)  # type: ignore
    for i, entry in enumerate(serialized):
        td = worker.deserialize(entry)
        if functions and td["f"] not in functions:
            continue
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
        if statuses and status not in statuses:
            continue
        dt = datetime.fromtimestamp((td.get("et") or td["t"]) / 1000, tz=worker.tz)
        tasks.append(
            TaskData(
                color=color,
                text_color=text_color,
                enqueue_time=dt.strftime("%Y-%m-%d %H:%M:%S"),
                status=status,
                task_id=task_id,
                fn_name=td["f"],
                sort_time=dt,
            )
        )
    tasks.sort(key=lambda td: td.sort_time)
    return templates.TemplateResponse(
        request,
        "table.j2",
        context={"tasks": tasks},
    )

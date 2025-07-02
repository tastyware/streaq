from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Form, Request, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel

from streaq import TaskPriority, TaskStatus, Worker
from streaq.constants import REDIS_RESULT, REDIS_RUNNING, REDIS_TASK
from streaq.ui.deps import get_worker, templates

router = APIRouter()


@router.get("/")
async def get_root() -> RedirectResponse:
    return RedirectResponse("/queue", status_code=status.HTTP_303_SEE_OTHER)


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
        count=1000,
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
            )
        )
    tasks.sort(key=lambda td: td.sort_time)
    return templates.TemplateResponse(
        request,
        "table.j2",
        context={"tasks": tasks},
    )


@router.get("/task/{task_id}", response_class=HTMLResponse)
async def get_task(
    request: Request, worker: Annotated[Worker[Any], Depends(get_worker)], task_id: str
) -> Any:
    status = await worker.status_by_id(task_id)
    if status == TaskStatus.DONE:
        result = await worker.result_by_id(task_id)
        function = result.fn_name
        enqueue_time = result.enqueue_time
        is_done = True
        start_dt = datetime.fromtimestamp(result.start_time / 1000, tz=worker.tz)
        end_dt = datetime.fromtimestamp(result.finish_time / 1000, tz=worker.tz)
        output, truncate_length = str(result.result), 32
        if len(output) > truncate_length:
            output = f"{output[:truncate_length]}…"
        extra = {
            "success": result.success,
            "result": output,
            "start_time": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "finish_time": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        }
    else:
        info = await worker.info_by_id(task_id)
        function = info.fn_name
        enqueue_time = info.enqueue_time
        is_done = False
        if info.scheduled:
            schedule = info.scheduled.strftime("%Y-%m-%d %H:%M:%S")
        else:
            schedule = None
        extra = {
            "task_try": info.task_try or 0,
            "scheduled": schedule,
            "dependencies": len(info.dependencies),
            "dependents": len(info.dependents),
        }
    if status == TaskStatus.DONE:
        color = "success"
        text_color = "light"
    elif status == TaskStatus.RUNNING:
        color = "warning"
        text_color = "dark"
    elif status == TaskStatus.SCHEDULED:
        color = "secondary"
        text_color = "light"
    else:
        color = "info"
        text_color = "dark"

    enqueue_dt = datetime.fromtimestamp(enqueue_time / 1000, tz=worker.tz)
    return templates.TemplateResponse(
        request,
        "task.j2",
        context={
            "color": color,
            "function": function,
            "is_done": is_done,
            "enqueue_time": enqueue_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "text_color": text_color,
            "title": "task",
            "status": status.value,
            "task_id": task_id,
            **extra,
        },
    )


@router.delete("/task/{task_id}")
async def abort_task(
    response: Response,
    worker: Annotated[Worker[Any], Depends(get_worker)],
    task_id: str,
) -> None:
    await worker.abort_by_id(task_id, timeout=3)
    response.headers["HX-Redirect"] = "/queue"

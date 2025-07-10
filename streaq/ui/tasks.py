from datetime import datetime
from typing import Annotated, Any

from async_lru import alru_cache
from fastapi import APIRouter, Depends, Form, Request, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel

from streaq import TaskStatus, Worker
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
    url: str


@alru_cache(ttl=1)
async def _get_context(worker: Worker[Any], task_url: str) -> dict[str, Any]:
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
                url=task_url.format(task_id=task_id),
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
    }


async def get_context(
    request: Request,
    worker: Worker[Any],
    functions: list[str] | None = None,
    statuses: list[TaskStatus] | None = None,
) -> dict[str, Any]:
    task_url = request.url_for("get_task", task_id="{task_id}").path
    tasks_filter_url = request.url_for("filter_tasks").path

    context = await _get_context(worker, task_url)
    context["tasks_filter_url"] = tasks_filter_url

    if functions:
        context["tasks"] = [t for t in context["tasks"] if t.fn_name in functions]
    if statuses:
        context["tasks"] = [t for t in context["tasks"] if t.status in statuses]

    return context


@router.get("/")
async def get_root(request: Request) -> RedirectResponse:
    url = request.url_for("get_tasks").path
    return RedirectResponse(url, status_code=status.HTTP_303_SEE_OTHER)


@router.get("/queue", response_class=HTMLResponse)
async def get_tasks(
    request: Request,
    worker: Annotated[Worker[Any], Depends(get_worker)],
) -> Any:
    context = await get_context(request, worker)
    return templates.TemplateResponse(request, "queue.j2", context=context)


@router.patch("/queue", response_class=HTMLResponse)
async def filter_tasks(
    request: Request,
    worker: Annotated[Worker[Any], Depends(get_worker)],
    functions: Annotated[list[str] | None, Form()] = None,
    statuses: Annotated[list[TaskStatus] | None, Form()] = None,
) -> Any:
    context = await get_context(request, worker, functions, statuses)
    return templates.TemplateResponse(request, "table.j2", context=context)


@router.get("/task/{task_id}", response_class=HTMLResponse)
async def get_task(
    request: Request,
    worker: Annotated[Worker[Any], Depends(get_worker)],
    task_id: str,
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
            "task_abort_url": request.url_for("abort_task", task_id=task_id).path,
            **extra,
        },
    )


@router.delete("/task/{task_id}")
async def abort_task(
    request: Request,
    response: Response,
    worker: Annotated[Worker[Any], Depends(get_worker)],
    task_id: str,
) -> None:
    await worker.abort_by_id(task_id, timeout=3)
    response.headers["HX-Redirect"] = request.url_for("get_tasks").path

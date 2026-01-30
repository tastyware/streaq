from datetime import datetime
from typing import Annotated, Any, Callable

from fastapi import (
    APIRouter,
    Depends,
    Form,
    HTTPException,
    Request,
    Response,
)
from fastapi import (
    status as fast_status,
)
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel

from streaq import TaskStatus, Worker
from streaq.constants import REDIS_RESULT, REDIS_RUNNING, REDIS_TASK
from streaq.ui.deps import (
    get_exception_formatter,
    get_result_formatter,
    get_worker,
    templates,
)
from streaq.utils import gather

router = APIRouter()
_fmt = "%Y-%m-%d %H:%M:%S"


class TaskData(BaseModel):
    color: str
    text_color: str
    enqueue_time: str
    task_id: str
    status: TaskStatus
    fn_name: str
    sort_time: datetime
    url: str


async def _get_context(
    worker: Worker[Any], task_url: str, descending: bool
) -> dict[str, Any]:
    async with worker.redis.pipeline(transaction=False) as pipe:
        delayed = [
            pipe.zrange(worker.queue_key + priority, 0, -1)
            for priority in worker.priorities
        ]
        commands = (
            pipe.xread(
                {worker.stream_key + p: "0-0" for p in worker.priorities},
                count=1000,
            ),
            pipe.keys(worker.prefix + REDIS_RESULT + "*"),
            pipe.keys(worker.prefix + REDIS_RUNNING + "*"),
            pipe.keys(worker.prefix + REDIS_TASK + "*"),
        )
    _stream, _results, _running, _data = await gather(*commands)
    stream: set[str] = (
        set(t.field_values["task_id"] for v in _stream.values() for t in v)  # type: ignore
        if _stream
        else set()
    )
    queue: set[str] = set()
    for r in await gather(*delayed):
        queue |= set(r)
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
        ts = td.get("ft") or td.get("t") or 0
        dt = datetime.fromtimestamp(ts / 1000, tz=worker.tz)
        tasks.append(
            TaskData(
                color=color,
                text_color=text_color,
                enqueue_time=dt.strftime(_fmt),
                status=status,
                task_id=task_id,
                fn_name=td["f"],
                sort_time=dt,
                url=task_url.format(task_id=task_id),
            )
        )
    tasks.sort(key=lambda td: td.sort_time, reverse=descending)
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
    sort: str = "desc",
) -> dict[str, Any]:
    task_url = request.url_for("get_task", task_id="{task_id}").path
    tasks_filter_url = request.url_for("filter_tasks").path

    descending = sort == "desc"
    context = await _get_context(worker, task_url, descending)
    context["tasks_filter_url"] = tasks_filter_url

    if functions:
        context["tasks"] = [t for t in context["tasks"] if t.fn_name in functions]
    if statuses:
        context["tasks"] = [t for t in context["tasks"] if t.status in statuses]

    context["tasks"] = context["tasks"][:100]
    return context


@router.get("/")
async def get_root(request: Request) -> RedirectResponse:
    url = request.url_for("get_tasks").path
    return RedirectResponse(url, status_code=fast_status.HTTP_303_SEE_OTHER)


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
    sort: Annotated[str, Form()],
    functions: Annotated[list[str] | None, Form()] = None,
    statuses: Annotated[list[TaskStatus] | None, Form()] = None,
) -> Any:
    context = await get_context(request, worker, functions, statuses, sort)
    return templates.TemplateResponse(request, "table.j2", context=context)


@router.get("/task/{task_id}", response_class=HTMLResponse)
async def get_task(
    request: Request,
    worker: Annotated[Worker[Any], Depends(get_worker)],
    result_formatter: Annotated[Callable[[Any], str], Depends(get_result_formatter)],
    exception_formatter: Annotated[
        Callable[[BaseException], str], Depends(get_exception_formatter)
    ],
    task_id: str,
) -> Any:
    status = await worker.status_by_id(task_id)
    if status == TaskStatus.DONE:
        result = await worker.result_by_id(task_id)
        function = result.fn_name
        created_time = result.created_time
        is_done = True
        start_dt = datetime.fromtimestamp(result.start_time / 1000, tz=worker.tz)
        end_dt = datetime.fromtimestamp(result.finish_time / 1000, tz=worker.tz)
        if result.success:
            output = result_formatter(result.result)
        else:
            output = exception_formatter(result.exception)
        task_try = result.tries
        worker_id = result.worker_id
        enqueue_dt = datetime.fromtimestamp(result.enqueue_time / 1000, tz=worker.tz)
        extra = {
            "enqueue_time": enqueue_dt.strftime(_fmt),
            "success": result.success,
            "result": output,
            "start_time": start_dt.strftime(_fmt),
            "finish_time": end_dt.strftime(_fmt),
        }
    else:
        info = await worker.info_by_id(task_id)
        if not info:
            raise HTTPException(
                status_code=fast_status.HTTP_404_NOT_FOUND, detail="Task not found!"
            )
        function = info.fn_name
        created_time = info.created_time
        worker_id = None
        is_done = False
        if info.scheduled:
            schedule = info.scheduled.strftime(_fmt)
        else:
            schedule = None
        task_try = info.tries
        extra = {
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

    created_dt = datetime.fromtimestamp(created_time / 1000, tz=worker.tz)
    return templates.TemplateResponse(
        request,
        "task.j2",
        context={
            "color": color,
            "function": function,
            "is_done": is_done,
            "created_time": created_dt.strftime(_fmt),
            "text_color": text_color,
            "title": "task",
            "status": status.value,
            "task_id": task_id,
            "task_abort_url": request.url_for("abort_task", task_id=task_id).path,
            "task_try": task_try,
            "worker_id": worker_id,
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

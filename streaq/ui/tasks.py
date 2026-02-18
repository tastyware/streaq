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

from streaq import TaskInfo, TaskResult, TaskStatus, Worker
from streaq.ui.deps import (
    get_exception_formatter,
    get_result_formatter,
    get_worker,
    templates,
)
from streaq.utils import gather

router = APIRouter()
_fmt = "%Y-%m-%d %H:%M:%S"

# Statuses to fetch
_STATUSES = (
    TaskStatus.SCHEDULED,
    TaskStatus.QUEUED,
    TaskStatus.RUNNING,
    TaskStatus.DONE,
)

# Status to color mapping
_STATUS_COLORS: dict[TaskStatus, tuple[str, str]] = {
    TaskStatus.DONE: ("success", "light"),
    TaskStatus.RUNNING: ("warning", "dark"),
    TaskStatus.SCHEDULED: ("secondary", "light"),
    TaskStatus.QUEUED: ("info", "dark"),
    TaskStatus.NOT_FOUND: ("danger", "light"),
}


def _get_sort_time(
    item: TaskInfo | TaskResult[Any], status: TaskStatus, tz: Any
) -> datetime:
    """Extract the appropriate datetime for sorting based on status."""
    if status == TaskStatus.SCHEDULED and isinstance(item, TaskInfo):
        if item.scheduled:
            return item.scheduled
        return datetime.fromtimestamp(item.created_time / 1000, tz=tz)
    elif status == TaskStatus.DONE and isinstance(item, TaskResult):
        return datetime.fromtimestamp(item.finish_time / 1000, tz=tz)
    else:
        return datetime.fromtimestamp(item.created_time / 1000, tz=tz)


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
    # Fetch all task types
    results = await gather(
        *(worker.get_tasks_by_status(s, limit=1000) for s in _STATUSES)
    )
    by_status = dict(zip(_STATUSES, results))

    tasks: list[TaskData] = []
    for status, items in by_status.items():
        color, text_color = _STATUS_COLORS[status]
        for item in items:
            dt = _get_sort_time(item, status, worker.tz)
            tasks.append(
                TaskData(
                    color=color,
                    text_color=text_color,
                    enqueue_time=dt.strftime(_fmt),
                    status=status,
                    task_id=item.task_id,
                    fn_name=item.fn_name,
                    sort_time=dt,
                    url=task_url.format(task_id=item.task_id),
                )
            )

    tasks.sort(key=lambda td: td.sort_time, reverse=descending)
    return {
        "running": len(by_status[TaskStatus.RUNNING]),
        "queued": len(by_status[TaskStatus.QUEUED]),
        "scheduled": len(by_status[TaskStatus.SCHEDULED]),
        "finished": len(by_status[TaskStatus.DONE]),
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

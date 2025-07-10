from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Form, Request, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse

from streaq import TaskStatus, Worker
from streaq.ui.context import get_context
from streaq.ui.deps import get_worker, templates

router = APIRouter()


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
    context = await get_context(request, worker)
    if functions:
        context["tasks"] = [t for t in context["tasks"] if t.fn_name in functions]
    if statuses:
        context["tasks"] = [t for t in context["tasks"] if t.status in statuses]
    return templates.TemplateResponse(request, "table.j2", context=context)


@router.get("/task/{task_id}", response_class=HTMLResponse)
async def get_task(
    request: Request,
    worker: Annotated[Worker[Any],
    Depends(get_worker)],
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

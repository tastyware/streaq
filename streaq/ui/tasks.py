from collections.abc import Callable
from datetime import datetime
from statistics import mean
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Form, HTTPException, Request, Response, status
from fastapi import status as fast_status
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel

from streaq import TaskStatus, Worker
from streaq.constants import REDIS_HEALTH
from streaq.task import TaskInfo, TaskResult
from streaq.ui import get_exception_formatter, get_result_formatter, get_worker
from streaq.ui.deps import templates

router = APIRouter()
_fmt = "%Y-%m-%d %H:%M:%S"

# Status to color mapping
_STATUS_COLORS: dict[TaskStatus, tuple[str, str]] = {
    TaskStatus.DONE: ("success", "light"),
    TaskStatus.RUNNING: ("warning", "dark"),
    TaskStatus.SCHEDULED: ("secondary", "light"),
    TaskStatus.QUEUED: ("info", "dark"),
    TaskStatus.DEAD: ("dark", "light"),
    TaskStatus.DEPENDS: ("light", "dark"),
}


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
    worker: Worker[Any], base_url: str, status: TaskStatus, priority: str | None
) -> dict[str, Any]:
    items: list[TaskInfo] | list[TaskResult[Any]]
    # only these statuses are stored per priority; split so each matches an overload
    if status in (TaskStatus.SCHEDULED, TaskStatus.QUEUED):  # noqa: SIM114
        items = await worker.get_tasks_by_status(status, priority=priority)
    elif status == TaskStatus.DEAD:
        items = await worker.get_tasks_by_status(status, priority=priority)
    else:
        items = await worker.get_tasks_by_status(status)
    async with worker.redis.pipeline(transaction=False) as pipe:
        delayed = [pipe.zcard(worker._queue_key + p) for p in worker.priorities]
        streams = [pipe.xlen(worker._stream_key + p) for p in worker.priorities]
        _dead = [pipe.zcard(worker._dead_key + p) for p in worker.priorities]
        _running = pipe.scard(worker._running_set)
        _depends = pipe.scard(worker._depends_set)
        results = pipe.zcard(worker._results_set)
    running, dead = await _running, sum([await d for d in _dead])
    color, text_color = _STATUS_COLORS[status]
    tasks: list[TaskData] = []
    for item in items:
        dt = datetime.fromtimestamp(item.created_time / 1000, tz=worker.tz)
        failed = status == TaskStatus.DONE and not item.success  # type: ignore
        tasks.append(
            TaskData(
                color="danger" if failed else color,
                text_color=text_color,
                enqueue_time=dt.strftime(_fmt),
                status=status,
                task_id=item.task_id,
                fn_name=item.fn_name,
                sort_time=dt,
                url=base_url + f"task/{item.task_id}",
            )
        )
    tasks.sort(key=lambda td: td.sort_time, reverse=True)
    return {
        "functions": list(worker.registry.keys()),
        "priorities": worker.priorities,
        "tasks": tasks,
        "title": worker.queue_name,
        "scheduled": sum([await d for d in delayed]),
        "queued": max(0, sum([await s for s in streams]) - running),
        "running": running,
        "done": max(0, await results - dead),
        "dead": dead,
        "depends": await _depends,
        "dlq": worker.dlq_ttl != 0,
        "is_dead": status == TaskStatus.DEAD,
        "is_abortable": status
        in (TaskStatus.SCHEDULED, TaskStatus.QUEUED, TaskStatus.RUNNING),
    }


async def get_context(
    request: Request,
    worker: Worker[Any],
    functions: list[str] | None = None,
    status: TaskStatus = TaskStatus.DONE,
    priority: str | None = None,
) -> dict[str, Any]:
    base_url = request.url_for("get_root").path
    context = await _get_context(worker, base_url, status, priority)
    context["base_url"] = base_url
    if functions:
        context["tasks"] = [t for t in context["tasks"] if t.fn_name in functions]

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
    status: Annotated[TaskStatus, Form()],
    functions: Annotated[list[str] | None, Form()] = None,
    priority: Annotated[str | None, Form()] = None,
) -> Any:
    context = await get_context(request, worker, functions, status, priority)
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
    if not status:
        raise HTTPException(
            status_code=fast_status.HTTP_404_NOT_FOUND, detail="Task not found!"
        )
    color, text_color = _STATUS_COLORS[status]
    info = await worker.info_by_id(task_id) if status != TaskStatus.DEAD else None
    if info and info.status != TaskStatus.DEAD:
        function = info.fn_name
        created_time = info.created_time
        worker_id = None
        is_done = False
        if info.scheduled:
            schedule = info.scheduled.strftime(_fmt)
        else:
            schedule = None
        task_try = info.tries
        extra = {"scheduled": schedule, "child": info.child}
    else:
        result = await worker.result_by_id(task_id, 3)
        function = result.fn_name
        created_time = result.created_time
        is_done = True
        start_dt = datetime.fromtimestamp(result.start_time / 1000, tz=worker.tz)
        end_dt = datetime.fromtimestamp(result.finish_time / 1000, tz=worker.tz)
        if result.success:
            output = result_formatter(result.result)
        else:
            color = "danger"
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

    created_dt = datetime.fromtimestamp(created_time / 1000, tz=worker.tz)
    return templates.TemplateResponse(
        request,
        "task.j2",
        context={
            "base_url": request.url_for("get_root").path,
            "color": color,
            "function": function,
            "is_done": is_done,
            "created_time": created_dt.strftime(_fmt),
            "text_color": text_color,
            "title": "task",
            "status": status.value,
            "task_id": task_id,
            "task_try": task_try,
            "worker_id": worker_id,
            "is_dead": status == TaskStatus.DEAD,
            **extra,
        },
    )


@router.delete("/task/{task_id}")
async def abort_task(
    request: Request,
    response: Response,
    worker: Annotated[Worker[Any], Depends(get_worker)],
    task_id: str,
    redirect: bool = True,
) -> None:
    if not await worker.abort_by_id(task_id, timeout=3):
        raise HTTPException(
            status_code=fast_status.HTTP_409_CONFLICT, detail="Task not aborted!"
        )
    if redirect:
        response.headers["HX-Redirect"] = request.url_for("get_tasks").path


@router.get("/workers", response_class=HTMLResponse)
async def get_workers(
    request: Request,
    worker: Annotated[Worker[Any], Depends(get_worker)],
) -> Any:
    worker_keys: list[str] = []
    cursor = 0
    while True:
        cursor, keys = await worker.redis.scan(
            cursor=cursor, match=worker._prefix + REDIS_HEALTH + "*", count=500
        ).route(worker.queue_name)
        worker_keys.extend(keys)
        if cursor == 0:
            break
    context: dict[str, Any] = {
        "base_url": request.url_for("get_root").path,
        "title": "workers",
    }
    if worker_keys:
        workers = await worker.redis.mget(worker_keys)
        context["all"] = [
            (key.split(":")[-1], " ".join(health.split(" ")[2:]))
            for key, health in zip(worker_keys, workers)
            if health
        ]
    tasks = await worker.get_tasks_by_status(TaskStatus.DONE, limit=1000)
    if tasks:
        async with worker.redis.pipeline(transaction=False) as pipe:
            _finished = pipe.zcard(worker._results_set)
            _dead = [pipe.zcard(worker._dead_key + p) for p in worker.priorities]
        finished, dead = await _finished, sum([await d for d in _dead])
        wait_times = [t.start_time - t.enqueue_time for t in tasks]
        tries = [t.tries for t in tasks]
        # 10 ms bounded slowdown
        slowdowns = [
            (t.finish_time - t.enqueue_time) / max(t.finish_time - t.start_time, 10)
            for t in tasks
        ]
        context["avg_wait_time"] = mean(wait_times)
        context["avg_slowdown"] = mean(slowdowns)
        context["avg_tries"] = mean(tries)
        # the sample excludes dead tasks, which are all failures, so scale it by
        # the share of finished tasks that aren't dead
        sample_rate = sum(t.success for t in tasks) / len(tasks)
        alive = max(finished - dead, 0) / max(finished, 1)
        context["success_rate"] = sample_rate * alive * 100
    return templates.TemplateResponse(request, "workers.j2", context=context)


@router.get("/cron", response_class=HTMLResponse)
async def get_cronjobs(
    request: Request,
    worker: Annotated[Worker[Any], Depends(get_worker)],
) -> Any:
    async with worker.redis.pipeline(transaction=True) as pipe:
        commands = (
            pipe.hgetall(worker._cron_registry_key),
            pipe.zrange(worker._cron_schedule_key, 0, -1, withscores=True),
        )
    registry, schedule = await commands[0], await commands[1]
    ordered_names = [str(k) for k, _ in schedule]
    return templates.TemplateResponse(
        request,
        "cron.j2",
        context={
            "base_url": request.url_for("get_root").path,
            "registry": {n: registry[n] for n in ordered_names},
            "schedule": {
                k: datetime.fromtimestamp(v / 1000, tz=worker.tz).strftime(_fmt)
                for k, v in schedule
            },
            "title": "cron",
        },
    )


@router.delete("/cron/{name}")
async def delete_cronjob(
    name: str, worker: Annotated[Worker[Any], Depends(get_worker)]
) -> None:
    res = await worker.unschedule_by_id(name)
    if not res:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Cron job not found!"
        )


@router.post("/task/{task_id}/requeue")
async def requeue_task(
    request: Request,
    response: Response,
    worker: Annotated[Worker[Any], Depends(get_worker)],
    task_id: str,
    redirect: bool = True,
) -> None:
    if not await worker.requeue_by_id(task_id):
        raise HTTPException(
            status_code=fast_status.HTTP_404_NOT_FOUND, detail="Task not found!"
        )
    if redirect:
        url = request.url_for("get_task", task_id=task_id).path
        response.headers["HX-Redirect"] = url


@router.delete("/task/{task_id}/purge")
async def purge_task(
    request: Request,
    response: Response,
    worker: Annotated[Worker[Any], Depends(get_worker)],
    task_id: str,
    redirect: bool = True,
) -> None:
    if not await worker.purge_by_id(task_id):
        raise HTTPException(
            status_code=fast_status.HTTP_404_NOT_FOUND, detail="Task not found!"
        )
    if redirect:
        response.headers["HX-Redirect"] = request.url_for("get_tasks").path

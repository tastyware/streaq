[![Docs](https://readthedocs.org/projects/streaq/badge/?version=latest)](https://streaq.readthedocs.io/en/latest/?badge=latest)
[![PyPI](https://img.shields.io/pypi/v/streaq)](https://pypi.org/project/streaq)
[![Downloads](https://static.pepy.tech/badge/streaq)](https://pepy.tech/project/streaq)
[![Release](https://img.shields.io/github/v/release/tastyware/streaq?label=release%20notes)](https://github.com/tastyware/streaq/releases)
![Coverage](https://raw.githubusercontent.com/tastyware/streaq/master/coverage.svg)
[![Human](https://img.shields.io/badge/human-coded-green?logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyNCIgaGVpZ2h0PSIyNCIgdmlld0JveD0iMCAwIDI0IDI0IiBmaWxsPSJub25lIiBzdHJva2U9IiNmZmZmZmYiIHN0cm9rZS13aWR0aD0iMiIgc3Ryb2tlLWxpbmVjYXA9InJvdW5kIiBzdHJva2UtbGluZWpvaW49InJvdW5kIiBjbGFzcz0ibHVjaWRlIGx1Y2lkZS1wZXJzb24tc3RhbmRpbmctaWNvbiBsdWNpZGUtcGVyc29uLXN0YW5kaW5nIj48Y2lyY2xlIGN4PSIxMiIgY3k9IjUiIHI9IjEiLz48cGF0aCBkPSJtOSAyMCAzLTYgMyA2Ii8+PHBhdGggZD0ibTYgOCA2IDIgNi0yIi8+PHBhdGggZD0iTTEyIDEwdjQiLz48L3N2Zz4=)](#)

# streaQ

Fast, async, fully-typed distributed task queue via Redis streams

## Features

- Up to [5x faster](https://github.com/tastyware/streaq/tree/master/benchmarks) than `arq`
- Fully typed
- Comprehensive documentation
- Support for delayed/scheduled tasks
- Cron jobs
- Task middleware
- Task dependency graph
- Pipelining
- Priority queues
- Support for synchronous tasks (run in separate threads)
- Redis Sentinel support for production
- Built-in web UI for monitoring tasks
- Built with structured concurrency on `anyio`, supports both `asyncio` and `trio`

> [!TIP]
> Sick of `redis-py`? Check out [coredis](https://coredis.readthedocs.io/en/latest/), a fully-typed Redis client that supports Trio!

## Installation

```console
$ pip install streaq
```

## Getting started

To start, you'll need to create a `Worker` object:

```python
from streaq import Worker

worker = Worker(redis_url="redis://localhost:6379")
```

You can then register async tasks with the worker like this:

```python
import trio

@worker.task()
async def sleeper(time: int) -> int:
    await trio.sleep(time)
    return time

@worker.cron("* * * * mon-fri")  # every minute on weekdays
async def cronjob() -> None:
    print("Nobody respects the spammish repetition!")
```

Finally, let's initialize the worker and queue up some tasks:

```python
async with worker:
    await sleeper.enqueue(3)
    # enqueue returns a task object that can be used to get results/info
    task = await sleeper.enqueue(1).start(delay=3)
    print(await task.info())
    print(await task.result(timeout=5))
```

Putting this all together gives us [example.py](https://github.com/tastyware/streaq/blob/master/example.py). Let's spin up a worker:
```
$ streaq run example:worker
```
and queue up some tasks like so:
```
$ python example.py
```

Let's see what the output looks like:

```
[INFO] 2025-09-23 02:14:30: starting worker 3265311d for 2 functions
[INFO] 2025-09-23 02:14:35: task sleeper □ cf0c55387a214320bd23e8987283a562 → worker 3265311d
[INFO] 2025-09-23 02:14:38: task sleeper ■ cf0c55387a214320bd23e8987283a562 ← 3
[INFO] 2025-09-23 02:14:40: task sleeper □ 1de3f192ee4a40d4884ebf303874681c → worker 3265311d
[INFO] 2025-09-23 02:14:41: task sleeper ■ 1de3f192ee4a40d4884ebf303874681c ← 1
[INFO] 2025-09-23 02:15:00: task cronjob □ 2a4b864e5ecd4fc99979a92f5db3a6e0 → worker 3265311d
Nobody respects the spammish repetition!
[INFO] 2025-09-23 02:15:00: task cronjob ■ 2a4b864e5ecd4fc99979a92f5db3a6e0 ← None
```
```python
TaskInfo(fn_name='sleeper', enqueue_time=1751508876961, tries=0, scheduled=datetime.datetime(2025, 7, 3, 2, 14, 39, 961000, tzinfo=datetime.timezone.utc), dependencies=set(), dependents=set())
TaskResult(fn_name='sleeper', enqueue_time=1751508876961, success=True, start_time=1751508880500, finish_time=1751508881503, tries=1, worker_id='ca5bd9eb', _result=1)
```

For more examples, check out the [documentation](https://streaq.readthedocs.io/en/latest/).

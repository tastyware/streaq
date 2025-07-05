[![Docs](https://readthedocs.org/projects/streaq/badge/?version=latest)](https://streaq.readthedocs.io/en/latest/?badge=latest)
[![PyPI](https://img.shields.io/pypi/v/streaq)](https://pypi.org/project/streaq)
[![Downloads](https://static.pepy.tech/badge/streaq)](https://pepy.tech/project/streaq)
[![Release)](https://img.shields.io/github/v/release/tastyware/streaq?label=release%20notes)](https://github.com/tastyware/streaq/releases)

# streaQ

Fast, async, type-safe distributed task queue via Redis streams

## Features

- Up to [5x faster](https://github.com/tastyware/streaq/tree/master/benchmarks) than arq
- Strongly typed
- 95%+ unit test coverage
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
import asyncio

@worker.task()
async def sleeper(time: int) -> int:
    await asyncio.sleep(time)
    return time

@worker.cron("* * * * mon-fri")  # every minute on weekdays
async def cronjob() -> None:
    print("Nobody respects the spammish repetition!")
```

Finally, use the worker's async context manager to queue up tasks:

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
$ streaq example.worker
```
and queue up some tasks like so:
```
$ python example.py
```

Let's see what the output looks like:

```
[INFO] 02:14:30: starting worker 3265311d for 2 functions
[INFO] 02:14:35: task cf0c55387a214320bd23e8987283a562 → worker 3265311d
[INFO] 02:14:38: task cf0c55387a214320bd23e8987283a562 ← 3
[INFO] 02:14:40: task 1de3f192ee4a40d4884ebf303874681c → worker 3265311d
[INFO] 02:14:41: task 1de3f192ee4a40d4884ebf303874681c ← 1
[INFO] 02:15:00: task 2a4b864e5ecd4fc99979a92f5db3a6e0 → worker 3265311d
Nobody respects the spammish repetition!
[INFO] 02:15:00: task 2a4b864e5ecd4fc99979a92f5db3a6e0 ← None
```
```
TaskInfo(fn_name='sleeper', enqueue_time=1751508876961, task_try=None, scheduled=datetime.datetime(2025, 7, 3, 2, 14, 39, 961000, tzinfo=datetime.timezone.utc), dependencies=set(), dependents=set())
TaskResult(fn_name='sleeper', enqueue_time=1751508876961, success=True, result=1, start_time=1751508880500, finish_time=1751508881503)
```

For more examples, check out the [documentation](https://streaq.readthedocs.io/en/latest/).

[![Docs](https://readthedocs.org/projects/streaq/badge/?version=latest)](https://streaq.readthedocs.io/en/latest/?badge=latest)
[![PyPI](https://img.shields.io/pypi/v/streaq)](https://pypi.org/project/streaq)
[![Downloads](https://static.pepy.tech/badge/streaq)](https://pepy.tech/project/streaq)
[![Release)](https://img.shields.io/github/v/release/tastyware/streaq?label=release%20notes)](https://github.com/tastyware/streaq/releases)

streaQ
======

Fast, async, type-safe job queuing with Redis streams

## Features

- Up to ?x faster than competitors
- 100% type safe
- 95%+ unit test coverage
- Comprehensive documentation
- Support for delayed tasks and cron jobs

## Installation

```console
$ pip install streaq
```

## Getting started

To start, you'll need to create a `Worker` object:

```python
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator
from httpx import AsyncClient
from streaq import Worker, WrappedContext

@dataclass
class Context:
    """
    Type safe way of defining the dependencies of your tasks.
    e.g. HTTP client, database connection, settings.
    """
    http_client: AsyncClient

@asynccontextmanager
async def worker_lifespan() -> AsyncIterator[Context]:
    async with AsyncClient() as http_client:
        yield Context(http_client)

worker = Worker(redis_url="redis://localhost:6379", worker_lifespan=worker_lifespan)
```

You can then register async tasks with the worker like this:

```python
@worker.task(timeout=5)
async def fetch(ctx: WrappedContext[Context], url: str) -> int:
    # ctx.deps here is of type Context, enforced by static typing
    # ctx also provides access the Redis connection, retry count, etc.
    r = await ctx.deps.http_client.get(url)
    return len(r.text)

@worker.cron("* * * * mon-fri")
async def cronjob(ctx: WrappedContext[Context]) -> None:
    print("It's a bird... It's a plane... It's CRON!")
```

Finally, use the worker's async context manager to queue up tasks:

```python
async with worker:
    await fetch.enqueue("https://tastyware.dev/")
    # this will be run directly locally, not enqueued
    await fetch.run("https://github.com/python-arq/arq")
    # enqueue returns a task object that can be used to get results/info
    task = await fetch.enqueue("https://github.com/tastyware/streaq").start(delay=3)
    print(await task.info())
    print(await task.result(timeout=5))
```

Putting this all together gives us [example.py](/blob/master/example.py). Let's spin up a worker:
```
$ streaq example.worker
```
and queue up some tasks like so:
```
$ python example.py
```

Let's see what the output looks like:

```
14:58:42: Starting worker 27c9a2413bdb44b6b4ea438d91db5954 for 2 functions...
14:58:43: redis_version=7.2.5 mem_usage=1.71M clients_connected=6 db_keys=4 queued=0 scheduled=0
14:58:49: Task 89a3deeb314e45198d85442c178e16c7 starting in worker 27c9a2413bdb44b6b4ea438d91db5954...
14:58:49: Task 89a3deeb314e45198d85442c178e16c7 finished.
14:58:53: Task 068313d7d792412e94a256985d988d7b starting in worker 27c9a2413bdb44b6b4ea438d91db5954...
14:58:53: Task 068313d7d792412e94a256985d988d7b finished.
14:59:00: Task cde2413d9593470babfd6d4e36cf4570 starting in worker 27c9a2413bdb44b6b4ea438d91db5954...
It's a bird... It's a plane... It's CRON!
14:59:00: Task cde2413d9593470babfd6d4e36cf4570 finished.
```
```
TaskData(fn_name='fetch', args=('https://github.com/tastyware/streaq',), kwargs={}, enqueue_time=1740081530008, task_id='068313d7d792412e94a256985d988d7b', task_try=None, scheduled=datetime.datetime(2025, 2, 20, 19, 58, 53, 8000, tzinfo=datetime.timezone.utc))
TaskResult(success=True, result=258795, start_time=1740081533010, finish_time=1740081533914, queue_name='streaq')
```

For more examples, check out the [documentation](https://streaq.readthedocs.io/en/latest/).

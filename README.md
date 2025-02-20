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
    # ctx also provides access to the Redis connection, retry count, etc.
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

Putting this all together gives us [example.py](/example.py). Let's spin up a worker:
```
$ streaq example.worker
```
and queue up some tasks like so:
```
$ python example.py
```

Let's see what the output looks like:

```
15:43:51: starting worker d5d4977b18694380bf36f837d6658b7b for 2 functions
15:43:52: redis_version=7.2.5 mem_usage=2.07M clients_connected=6 db_keys=4 queued=0 scheduled=0
15:43:54: task d0b8ea4dc4c1442486f69926a042b024 → worker d5d4977b18694380bf36f837d6658b7b
15:43:55: task d0b8ea4dc4c1442486f69926a042b024 ← 15
15:43:58: task f00f6406d663448fa63ad1f1a79f71c8 → worker d5d4977b18694380bf36f837d6658b7b
15:43:59: task f00f6406d663448fa63ad1f1a79f71c8 ← 295022
15:44:00: task cde2413d9593470babfd6d4e36cf4570 → worker d5d4977b18694380bf36f837d6658b7b
It's a bird... It's a plane... It's CRON!
15:44:00: task cde2413d9593470babfd6d4e36cf4570 ← None
```
```
TaskData(fn_name='fetch', args=('https://github.com/tastyware/streaq',), kwargs={}, enqueue_time=1740084235761, task_id='f00f6406d663448fa63ad1f1a79f71c8', task_try=None, scheduled=datetime.datetime(2025, 2, 20, 20, 43, 58, 761000, tzinfo=datetime.timezone.utc))
TaskResult(success=True, result=295022, start_time=1740084238762, finish_time=1740084239756, queue_name='streaq')
```

For more examples, check out the [documentation](https://streaq.readthedocs.io/en/latest/).

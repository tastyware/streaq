[![Docs](https://readthedocs.org/projects/streaq/badge/?version=latest)](https://streaq.readthedocs.io/en/latest/?badge=latest)
[![PyPI](https://img.shields.io/pypi/v/streaq)](https://pypi.org/project/streaq)
[![Downloads](https://static.pepy.tech/badge/streaq)](https://pepy.tech/project/streaq)
[![Release)](https://img.shields.io/github/v/release/tastyware/streaq?label=release%20notes)](https://github.com/tastyware/streaq/releases)

# streaQ

Fast, async, type-safe job queuing with Redis streams

## Features

- Up to [5x-15x faster](https://github.com/tastyware/streaq/tree/master/benchmarks) than arq
- Strongly typed
- 95%+ unit test coverage
- Comprehensive documentation
- Support for delayed/scheduled tasks
- Cron jobs
- Task middleware
- Task dependency graph
- Task pipelines
- Task priority queues
- Support for synchronous tasks (run in separate threads)
- Dead simple, ~2k lines of code
- Redis Sentinel support for production

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
async def lifespan(worker: Worker) -> AsyncIterator[Context]:
    async with AsyncClient() as http_client:
        yield Context(http_client)

worker = Worker(redis_url="redis://localhost:6379", lifespan=lifespan)
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
[INFO] 19:49:44: starting worker db064c92 for 3 functions
[INFO] 19:49:46: task dc844a5b5f394caa97e4c6e702800eba → worker db064c92
[INFO] 19:49:46: task dc844a5b5f394caa97e4c6e702800eba ← 15
[INFO] 19:49:50: task 178c4f4e057942d6b6269b38f5daaaa1 → worker db064c92
[INFO] 19:49:50: task 178c4f4e057942d6b6269b38f5daaaa1 ← 293784
[INFO] 19:50:00: task a0a8c0f39dae4c448182c417b047677c → worker db064c92
[INFO] 19:50:00: task cde2413d9593470babfd6d4e36cf4570 → worker db064c92
It's a bird... It's a plane... It's CRON!
[INFO] 19:50:00: task cde2413d9593470babfd6d4e36cf4570 ← None
[INFO] 19:50:00: health check results:
redis {memory: 1.72M, clients: 3, keys: 18, queued: 2, scheduled: 0}
worker db064c92 {completed: 2}
[INFO] 19:50:00: task a0a8c0f39dae4c448182c417b047677c ← None
```
```
TaskData(fn_name='fetch', enqueue_time=1743468587037, task_try=None, scheduled=datetime.datetime(2025, 4, 1, 0, 49, 50, 37000, tzinfo=datetime.timezone.utc))
TaskResult(success=True, result=293784, start_time=1743468590041, finish_time=1743468590576, queue_name='default')
```

For more examples, check out the [documentation](https://streaq.readthedocs.io/en/latest/).

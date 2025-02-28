Getting started
===============

To start, you'll need to create a ``Worker`` object:

.. code-block:: python

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
   async def worker_lifespan(worker: Worker) -> AsyncIterator[Context]:
       async with AsyncClient() as http_client:
           yield Context(http_client)

   worker = Worker(redis_url="redis://localhost:6379", worker_lifespan=worker_lifespan)

You can then register async tasks with the worker like this:

.. code-block:: python

   @worker.task(timeout=5)
   async def fetch(ctx: WrappedContext[Context], url: str) -> int:
       # ctx.deps here is of type Context, enforced by static typing
       # ctx also provides access to the Redis connection, retry count, etc.
       r = await ctx.deps.http_client.get(url)
       return len(r.text)

   @worker.cron("* * * * mon-fri")
   async def cronjob(ctx: WrappedContext[Context]) -> None:
       print("It's a bird... It's a plane... It's CRON!")

Finally, use the worker's async context manager to queue up tasks:

.. code-block:: python

   async with worker:
       await fetch.enqueue("https://tastyware.dev/")
       # this will be run directly locally, not enqueued
       await fetch.run("https://github.com/python-arq/arq")
       # enqueue returns a task object that can be used to get results/info
       task = await fetch.enqueue("https://github.com/tastyware/streaq").start(delay=3)
       print(await task.info())
       print(await task.result(timeout=5))

Putting this all together gives us `example.py <https://github.com/tastyware/streaq/blob/master/example.py>`_. Let's spin up a worker:

.. code-block:: bash

   $ streaq example.worker

and queue up some tasks like so:

.. code-block:: bash

   $ python example.py

Let's see what the output looks like:

.. code-block::

   13:25:08: starting worker 0cb8bb10870442e4ba0543c8c5effd29 for 2 functions
   13:25:08: redis_version=7.2.5 mem_usage=1.98M clients_connected=6 db_keys=8 queued=0 scheduled=0
   13:25:11: task dba141e367f949589fc67d1a12e0f1a5 → worker 0cb8bb10870442e4ba0543c8c5effd29
   13:25:12: task dba141e367f949589fc67d1a12e0f1a5 ← 15
   13:25:16: task 62f5671e7cde44d1bb26cd1fc16d126e → worker 0cb8bb10870442e4ba0543c8c5effd29
   13:25:17: task 62f5671e7cde44d1bb26cd1fc16d126e ← 294815
   13:26:00: task cde2413d9593470babfd6d4e36cf4570 → worker 0cb8bb10870442e4ba0543c8c5effd29
   It's a bird... It's a plane... It's CRON!
   13:26:00: task cde2413d9593470babfd6d4e36cf4570 ← None

.. code-block:: python

   TaskData(fn_name='fetch', enqueue_time=1740162312862, task_try=None, scheduled=datetime.datetime(2025, 2, 21, 18, 25, 15, 862000, tzinfo=datetime.timezone.utc))
   TaskResult(success=True, result=294815, start_time=1740162316157, finish_time=1740162317140, queue_name='streaq')

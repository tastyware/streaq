Getting started
===============

To start, you'll need to create a ``Worker`` object. At worker creation, you can provide an async context manager "lifespan" which will initialize any global dependencies you want to have access to in your tasks:

.. code-block:: python

   from contextlib import asynccontextmanager
   from dataclasses import dataclass
   from typing import AsyncGenerator
   from httpx import AsyncClient
   from streaq import Worker

   @dataclass
   class WorkerContext:
       """
       Type safe way of defining the dependencies of your tasks.
       e.g. HTTP client, database connection, settings.
       """
       http_client: AsyncClient

   @asynccontextmanager
   async def lifespan() -> AsyncGenerator[WorkerContext]:
       """
       Here, we initialize the worker's dependencies.
       You can also do any startup/shutdown work here
       """
       async with AsyncClient() as http_client:
           yield WorkerContext(http_client)

   worker = Worker(redis_url="redis://localhost:6379", lifespan=lifespan)

You can then register async tasks with the worker like this:

.. code-block:: python

   @worker.task(timeout=5)
   async def fetch(url: str) -> int:
       # worker.context here is of type WorkerContext
       res = await worker.context.http_client.get(url)
       return len(res.text)

Finally, let's queue up some tasks via the worker's async context manager:

.. code-block:: python

    async with worker:
        await fetch.enqueue("https://tastyware.dev/")
        # enqueue returns a task object that can be used to get results/info
        task = await fetch.enqueue("https://github.com/tastyware/streaq").start(delay=3)
        print(await task.info())
        print(await task.result(timeout=5))

Put this all together in a script and spin up a worker:

.. code-block:: bash

   $ streaq script.worker

and queue up some tasks like so:

.. code-block:: bash

   $ python script.py

Let's see what the output looks like:

.. code-block::

   [INFO] 2025-09-23 07:19:48: task fetch □ 45d7ff032e6d42239e9f479a2fc4b70e → worker 12195ce1
   [INFO] 2025-09-23 07:19:48: task fetch ■ 45d7ff032e6d42239e9f479a2fc4b70e ← 15
   [INFO] 2025-09-23 07:19:51: task fetch □ 65e687f9ba644a1fbe23096fa246dfe1 → worker 12195ce1
   [INFO] 2025-09-23 07:19:52: task fetch ■ 65e687f9ba644a1fbe23096fa246dfe1 ← 303659

.. code-block:: python

   TaskInfo(fn_name='fetch', enqueue_time=1756365588232, tries=0, scheduled=datetime.datetime(2025, 8, 28, 7, 19, 51, 232000, tzinfo=datetime.timezone.utc), dependencies=set(), dependents=set())
   TaskResult(fn_name='fetch', enqueue_time=1756365588232, success=True, result=303659, start_time=1756365591327, finish_time=1756365592081, tries=1, worker_id='12195ce1')

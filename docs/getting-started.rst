Getting started
===============

To start, you'll need to create a ``Worker`` object. At worker creation, you can provide an async context manager "lifespan" which will initialize any global dependencies you want tasks to have access to:

.. code-block:: python

   from contextlib import asynccontextmanager
   from dataclasses import dataclass
   from typing import AsyncIterator
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
   async def lifespan(worker: Worker[WorkerContext]) -> AsyncIterator[WorkerContext]:
       """
       Here, we initialize the worker's dependencies.
       You can also do any startup/shutdown work here
       """
       async with AsyncClient() as http_client:
           yield Context(http_client)

   worker = Worker(redis_url="redis://localhost:6379", lifespan=lifespan)

You can then register async tasks with the worker like this:

.. code-block:: python

   @worker.task(timeout=5)
   async def fetch(url: str) -> int:
       # worker.context here is of type WorkerContext
       res = await worker.context.http_client.get(url)
       return len(res.text)

Finally, use the worker's async context manager to queue up tasks:

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

   [INFO] 13:18:07: starting worker ca5bd9eb for 2 functions
   [INFO] 13:18:10: task 1ab9543aae374bd89713ca00f5c566f9 → worker ca5bd9eb
   [INFO] 13:18:10: task 1ab9543aae374bd89713ca00f5c566f9 ← 15
   [INFO] 13:18:14: task cac277a9e3034704a36e67099c1d6f07 → worker ca5bd9eb
   [INFO] 13:18:15: task cac277a9e3034704a36e67099c1d6f07 ← 303557

.. code-block:: python

   TaskInfo(fn_name='fetch', enqueue_time=1751635090933, task_try=None, scheduled=datetime.datetime(2025, 7, 4, 13, 18, 13, 933000, tzinfo=datetime.timezone.utc), dependencies=set(), dependents=set())
   TaskResult(fn_name='fetch', enqueue_time=1751635090933, success=True, result=303557, start_time=1751635094068, finish_time=1751635095130)

Getting started
===============

To start, you'll need to define your global dependencies which your tasks will need access to at run time. This is done with a ``dataclass`` or ``NamedTuple``:

.. code-block:: python
   :caption: worker.py

   from dataclasses import dataclass
   from httpx import AsyncClient

   @dataclass
   class WorkerContext:
       """
       Type safe way of defining the dependencies of your tasks.
       e.g. HTTP client, database connection, settings.
       """
       http_client: AsyncClient

Now, when creating a ``Worker`` object, you can provide an async context manager "lifespan" which will initialize any global dependencies you want to have access to in your tasks:

.. code-block:: python
   :caption: worker.py
   :emphasize-lines: 1,3,5,15-24

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
   async def lifespan() -> AsyncGenerator[WorkerContext, None]:
       """
       Here, we initialize the worker's dependencies.
       You can also do any startup/shutdown work here
       """
       async with AsyncClient() as http_client:
           yield WorkerContext(http_client)

   my_worker = Worker(redis_url="redis://localhost:6379", lifespan=lifespan)

You can then register async tasks to the worker like this:

.. code-block:: python
   :caption: worker.py
   :emphasize-lines: 5,26-29

   from contextlib import asynccontextmanager
   from dataclasses import dataclass
   from typing import AsyncGenerator
   from httpx import AsyncClient
   from streaq import Worker, WorkerDepends

   @dataclass
   class WorkerContext:
       """
       Type safe way of defining the dependencies of your tasks.
       e.g. HTTP client, database connection, settings.
       """
       http_client: AsyncClient

   @asynccontextmanager
   async def lifespan() -> AsyncGenerator[WorkerContext, None]:
       """
       Here, we initialize the worker's dependencies.
       You can also do any startup/shutdown work here
       """
       async with AsyncClient() as http_client:
           yield WorkerContext(http_client)

   my_worker = Worker(redis_url="redis://localhost:6379", lifespan=lifespan)

   @my_worker.task(timeout=5)
   async def fetch(url: str, ctx: WorkerContext = WorkerDepends()) -> int:
       res = await ctx.http_client.get(url)
       return len(res.text)

Now let's save the file and spin up a worker which will pick up future tasks:

.. code-block:: bash

   $ streaq run worker:my_worker

.. note::
   The worker path format for the CLI is ``module.submodule:object``.

Finally, let's create a script to queue up some tasks via the worker's async context manager:

.. code-block:: python
   :caption: script.py

   from anyio import run
   from script import my_worker, fetch

   async def main():
       async with my_worker:
           await fetch.enqueue("https://tastyware.dev/")
           # enqueue returns a task object that can be used to get results/info
           task = await fetch.enqueue("https://github.com/tastyware/streaq").start(delay=3)
           print(await task.info())
           print(await task.result(timeout=5))

   run(main)  # necessary for async code, or you could use an async REPL

We can run this script with ``$ python script.py``. You should see output like:

.. code-block:: python

   TaskInfo(fn_name='fetch', enqueue_time=1756365588232, tries=0, scheduled=datetime.datetime(2025, 8, 28, 7, 19, 51, 232000, tzinfo=datetime.timezone.utc), dependencies=set(), dependents=set())
   TaskResult(fn_name='fetch', enqueue_time=1756365588232, success=True, start_time=1756365591327, finish_time=1756365592081, tries=1, worker_id='12195ce1', _result=303659)

And the worker logs should look like this:

.. code-block::

   [INFO] 2025-09-23 07:19:48: task fetch □ 45d7ff032e6d42239e9f479a2fc4b70e → worker 12195ce1
   [INFO] 2025-09-23 07:19:48: task fetch ■ 45d7ff032e6d42239e9f479a2fc4b70e ← 15
   [INFO] 2025-09-23 07:19:51: task fetch □ 65e687f9ba644a1fbe23096fa246dfe1 → worker 12195ce1
   [INFO] 2025-09-23 07:19:52: task fetch ■ 65e687f9ba644a1fbe23096fa246dfe1 ← 303659

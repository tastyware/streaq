Framework integrations
======================

FastAPI
-------

Integration with FastAPI is straightforward:

.. code-block:: python

   from typing import AsyncGenerator

   from fastapi import Depends, FastAPI
   from streaq import Worker

   from example import fetch, worker

   app = FastAPI()

   async def get_worker() -> AsyncGenerator[Worker, None]:
       async with worker:
           yield worker

   @app.post("/enqueue", dependencies=[Depends(get_worker)])
   async def enqueue(url: str) -> bool:
       task = await fetch.enqueue(url)
       res = await task.result(5)
       return res.success

Here, we're building off of the ``fetch`` task and ``worker`` instance defined in :doc:`Getting started <getting-started>`. But what if the backend doesn't have access to the task definitions?

Separating enqueuing from task definitions
------------------------------------------

A common scenario is to have separate codebases for the backend and the worker. For example, if your worker is serving a large LLM, you probably don't want to load the LLM in the backend. There are two ways to handle this:

First, you can simply use type stubs to re-define the task signatures in the backend:

.. code-block:: python

   from streaq import Worker

   # this worker should have the same Redis URL, serializer/deserializer,
   # and queue name as the worker defined elsewhere
   worker = Worker(redis_url="redis://localhost:6379")

   @worker.task()
   async def fetch(ctx, url: str) -> int: ...

Now, tasks can be enqueued in the same way as before:

.. code-block:: python

   async with worker:
       await fetch.enqueue("https://github.com/tastyware/streaq")

.. warning::

   ``fetch.run()`` will not work here, since ``run()`` skips enqueuing entirely!

The second way is to use ``Worker.enqueue_unsafe``:

.. code-block:: python

   from streaq import Worker

   # again, this worker should have the same Redis URL, serializer/deserializer,
   # and queue name as the worker defined elsewhere
   worker = Worker(redis_url="redis://localhost:6379")

   async with worker:
       await worker.enqueue_unsafe("fetch", "https://tastyware.dev")

This method is not type-safe, but it doesn't require you to re-define the task signature in the backend. Here, the first parameter is the ``fn_name`` of the task defined elsewhere, and the rest of the args and kwargs can be passed normally.

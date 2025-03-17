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

TODO: add unsafe enqueue example



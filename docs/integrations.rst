Framework integrations
======================

FastAPI
-------

Integration with FastAPI is straightforward:

.. code-block:: python

   from fastapi import FastAPI, HTTPException, status

   from example import fetch

   @asynccontextmanager
   async def app_lifespan(app: FastAPI) -> AsyncGenerator[None]:
       async with worker:
           yield

   app = FastAPI(lifespan=app_lifespan)

   @app.post("/fetch")
   async def do_fetch(url: str) -> int:
       task = await fetch.enqueue(url)
       try:
           res = await task.result(5)
       except TimeoutError as e:
           raise HTTPException(
               status_code=status.HTTP_408_REQUEST_TIMEOUT, detail="Timed out!"
           )
       if not res.success:
           raise HTTPException(
               status_code=status.HTTP_424_FAILED_DEPENDENCY, detail="Task failed!"
           )
       return res.result

Here, we're building off of the ``fetch`` task defined in :doc:`Getting started <getting-started>`. As you can imagine, integrating with other frameworks should be very similar!

Web UI integration
------------------

The web UI is useful for monitoring tasks; however, the information available there (and the ability to cancel tasks) is probably not something you want to make available to all your users.

With a little work the UI can be mounted as a part of an existing FastAPI application. You just need to override the ``get_worker()`` dependency, then your can integrate the UI into your existing app:

.. code-block:: python

   from streaq.ui import get_worker, router

   app = FastAPI(lifespan=app_lifespan)  # see above, we need the worker to be initialized
   app.dependency_overrides[get_worker] = lambda: worker
   # here, you can add any auth-related dependencies as well
   app.include_router(router, prefix="/streaq", dependencies=[...])

If desired, you can add custom formatters in a similar way:

.. code-block:: python

   from streaq.ui import get_result_formatter, get_exception_formatter

   def my_result_formatter(result: Any) -> str: ...
   def my_exception_formatter(exc: BaseException) -> str: ...

   app.dependency_overrides[get_result_formatter] = lambda: my_result_formatter
   app.dependency_overrides[get_exception_formatter] = lambda: my_exception_formatter

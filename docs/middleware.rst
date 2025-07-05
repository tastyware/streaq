Middleware
==========

Creating middleware
-------------------

You can define middleware to wrap task execution. This has a host of potential applications, like observability and exception handling. Here's an example which times function execution:

.. code-block:: python

   import time
   from streaq.types import ReturnCoroutine
   from typing import Any

   @worker.middleware
   def timer(task: ReturnCoroutine) -> ReturnCoroutine:
       async def wrapper(*args, **kwargs) -> Any:
           start_time = time.perf_counter()
           result = await task(*args, **kwargs)
           print(f"Executed task {ctx.task_id} in {time.perf_counter() - start_time:.3f}s")
           return result

       return wrapper

Middleware are structured as wrapped functions for maximum flexibility--not only can you run code before/after execution, you can also access and even modify the arguments or results.

Stacking middleware
-------------------

You can register as many middleware as you like to a worker, which will run them in the same order they were registered.

.. code-block:: python

   from streaq import StreaqRetry

   @worker.middleware
   def timer(task: ReturnCoroutine) -> ReturnCoroutine:
       async def wrapper(*args, **kwargs) -> Any:
           start_time = time.perf_counter()
           result = await task(*args, **kwargs)
           print(f"Executed task {ctx.task_id} in {time.perf_counter() - start_time:.3f}s")
           return result

       return wrapper

   # retry all exceptions up to a max of 3 tries
   @worker.middleware
   def retry(task: ReturnCoroutine) -> ReturnCoroutine:
       async def wrapper(*args, **kwargs) -> Any:
           try:
               return await task(*args, **kwargs)
           except Exception as e:
               try_count = worker.task_context().tries
               if try_count < 3:
                   raise StreaqRetry("Retrying on error!") from e
               else:
                   raise e

       return wrapper

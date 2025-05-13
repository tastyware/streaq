Middleware
==========

Creating middleware
-------------------

You can define middleware to wrap task execution. This has a host of potential applications, like observability and exception handling. Here's an example which times function execution:

.. code-block:: python

   import time
   from typing import Callable, Coroutine

   @worker.middleware
   def timer(ctx: WrappedContext[Context], task: Callable[..., Coroutine]):
       async def wrapper(*args, **kwargs):
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

   import time
   from typing import Callable, Coroutine
   from streaq import StreaqRetry

   @worker.middleware
   def timer(ctx: WrappedContext[Context], task: Callable[..., Coroutine]):
       async def wrapper(*args, **kwargs):
           start_time = time.perf_counter()
           result = await task(*args, **kwargs)
           print(f"Executed task {ctx.task_id} in {time.perf_counter() - start_time:.3f}s")
           return result

       return wrapper

   @worker.middleware
   def retry(ctx: WrappedContext[Context], task: Callable[..., Coroutine]):
       async def wrapper(*args, **kwargs):
           try:
               return await task(*args, **kwargs)
           except Exception:
               raise StreaqRetry("Retrying on error!")

       return wrapper

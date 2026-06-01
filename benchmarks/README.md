# Benchmarks

streaQ's performance significantly improves upon [arq](https://github.com/python-arq/arq), and is on-par with [taskiq](https://github.com/taskiq-python/taskiq). If you want to run these tests yourself, first install the dependencies:
```
$ uv pip install git+https://github.com/Graeme22/arq.git
$ uv pip install "taskiq-redis==1.2.2"
```

You can enqueue jobs like so:
```
$ python benchmarks/bench_streaq.py --time 1
```

And run a worker with one of these commands, adjusting the number of workers as desired:
```
$ arq --workers ? --burst bench_arq.WorkerSettings
$ streaq run --burst --workers ? bench_streaq:worker
$ taskiq worker --workers ? --max-async-tasks 32 bench_taskiq:broker --max-prefetch 32
```

These benchmarks were run with streaQ v7.0.0 on an M4 Mac Mini using asyncio + uvloop. Trio performance is slightly worse.

## Benchmark 1: No-op

This benchmark evaluates the performance when tasks do nothing, representing negligible amounts of work.
These results are with 20,000 tasks enqueued, concurrency of `32`, and a variable number of workers.

| library  | enqueuing | 1 worker | 10 workers |
| -------- | --------- | -------- | ---------- |
| streaq   | 0.57s     | 5.54s    | 2.45s      |
| taskiq   | 1.60s     | 4.60s    | 3.00s      |
| arq      | 1.32s     | 62.06s   | 35.88s     |

## Benchmark 2: Sleep

This benchmark evaluates the performance when tasks sleep for 1 second, representing a small amount of work.
These results are with 20,000 tasks enqueued, concurrency of `32`, and a variable number of workers.

| library  | enqueuing | 10 workers | 100 workers |
| -------- | --------- | ---------- | ----------- |
| streaq   | 0.57s     | 64.20s     | 7.90s       |
| taskiq   | 1.60s     | 63.57s     | 12.19s      |
| arq      | 1.32s     | 178.96s    | 285.99s     |

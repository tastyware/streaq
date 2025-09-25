# Benchmarks

streaQ's performance significantly improves upon [arq](https://github.com/python-arq/arq), and is on-par with [SAQ](https://github.com/tobymao/saq) and [taskiq](https://github.com/taskiq-python/taskiq). If you want to run these tests yourself, first install the dependencies:
```
$ pip install streaq[benchmark]
```

You can enqueue jobs like so:
```
$ python benchmarks/bench_streaq.py --time 1
```

And run a worker with one of these commands, adjusting the number of workers as desired:
```
$ arq --workers ? --burst bench_arq.WorkerSettings
$ saq --quiet bench_saq.settings --workers ?
$ streaq --burst --workers ? bench_streaq.worker
$ taskiq worker --workers ? --max-async-tasks 32 bench_taskiq:broker --max-prefetch 32
```

These benchmarks were run with streaQ v6.0.0 on an M4 Mac Mini using asyncio + uvloop. Trio performance is slightly worse.

## Benchmark 1: No-op

This benchmark evaluates the performance when tasks do nothing, representing negligible amounts of work.
These results are with 20,000 tasks enqueued, a concurrency of `32`, and a variable number of workers.

| library  | enqueuing | 1 worker | 10 workers | 20 workers | 40 workers |
| -------- | --------- | -------- | ---------- | ---------- | ---------- |
| streaq   | 0.45s     | 8.50s    | 3.84s      | 4.19s      | 5.18s      |
| SAQ      | 1.67s     | 9.86s    | 3.46s      | 3.45s      | 3.93s      |
| taskiq   | 1.68s     | 6.36s    | 3.26s      | 3.38s      | 6.43s      |
| arq      | 2.31s     | 62.66s   | 28.10s     | 43.33s     | ☠️         |

## Benchmark 2: Sleep

This benchmark evaluates the performance when tasks sleep for 1 second, representing a small amount of work.
These results are with 20,000 tasks enqueued, a concurrency of `32`, and a variable number of workers.

| library  | enqueuing | 10 workers | 20 workers | 40 workers |
| -------- | --------- | ---------- | ---------- | ---------- |
| streaq   | 0.44s     | 63.89s     | 33.02s     | 17.33s     |
| SAQ      | 1.69s     | 64.51s     | 33.56s     | 17.74s     |
| taskiq   | 1.68s     | 67.53s     | 34.42s     | 18.55s     |
| arq      | 2.27s     | 176.87s    | 169.47s    | ☠️         |

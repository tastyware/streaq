# Benchmarks

streaQ's performance significantly improves upon [arq](https://github.com/python-arq/arq), and is on-par with or slightly better than [SAQ](https://github.com/tobymao/saq). It also outperforms the popular [taskiq](https://github.com/taskiq-python/taskiq)! If you want to run these tests yourself, first install the dependencies:
```
$ pip install streaq[benchmark]
```

You can enqueue jobs like so:
```
$ python benchmarks/bench_streaq.py --time 1
```

And run a worker with one of these commands, adjusting the number of workers as desired:
```
$ arq --workers ? bench_arq.WorkerSettings
$ saq --quiet bench_saq.settings --workers ?
$ streaq --burst --workers ? bench_streaq.worker
$ taskiq worker --workers ? --max-async-tasks 32 bench_taskiq:broker --max-prefetch 32
```

These benchmarks were run with streaQ v0.6.0.

## Benchmark 1: No-op

This benchmark evaluates the performance when tasks do nothing, representing negligible amounts of work.
These results are with 20,000 tasks enqueued, a concurrency of `32`, and a variable number of workers.

| library  | enqueuing | 1 worker | 10 workers | 20 workers | 40 workers |
| -------- | --------- | -------- | ---------- | ---------- | ---------- |
| streaq   | 2.31s     | 12.57s   | 3.70s      | 4.05s      | 4.13s      |
| SAQ      | 2.97s     | 11.71s   | 3.38s      | 4.06s      | 4.73s      |
| taskiq   | 4.76s     | 13.69s   | 9.35s      | 10.45s     | 10.85s     |
| arq      | 4.63s     | 57.82s   | 53.96s     | 35.84s     | 19.78s     |

## Benchmark 2: Sleep

This benchmark evaluates the performance when tasks sleep for 1 second, representing a small amount of work.
These results are with 20,000 tasks enqueued, a concurrency of `32`, and a variable number of workers.

| library  | enqueuing | 1 worker | 10 workers | 20 workers | 40 workers |
| -------- | --------- | -------- | ---------- | ---------- | ---------- |
| streaq   | 2.57s     | 627.23s  | 63.87s     | 33.08s     | 18.94s     |
| SAQ      | 2.93s     | 642.09s  | 64.95s     | 33.66s     | 17.81s     |
| taskiq   | 4.63s     | 634.21s  | 150.52s    | 124.59s    | 87.70s     |
| arq      | 4.82s     | 933.67s  | 173.58s    | 173.01s    | 142.53s    |

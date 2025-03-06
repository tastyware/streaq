# Benchmarks

streaQ's performance significantly improves upon [arq](https://github.com/python-arq/arq), and is on-par or slightly better than [SAQ](https://github.com/tobymao/saq). If you want to run these tests yourself, first install the dependencies:
```
$ pip install streaq[benchmark]
```

You can enqueue jobs like so:
```
$ python benchmarks/bench_streaq.py --time 1
```

And run a worker with one of these commands, adjusting the number of workers as desired:
```
$ arq --workers ? benchmarks.bench_arq.WorkerSettings
$ saq --quiet benchmarks.bench_saq.settings --workers ?
$ streaq --burst --workers ? benchmarks.baseline.worker
```

## Benchmark 1: No-op

This benchmark evaluates the performance when tasks do nothing, representing negligible amounts of work.
These results are with 20,000 tasks enqueued, a concurrency of `32`, and a variable number of workers.

| library  | enqueuing | 1 worker | 10 workers | 20 workers | 40 workers |
| -------- | --------- | -------- | ---------- | ---------- | ---------- |
| streaq   | 2.31s     | 12.19s   | 3.34s      | 3.56s      | 3.77s      |
| arq      | 4.63s     | 57.82s   | 53.96s     | 35.84s     | 19.78s     |
| SAQ      | 2.97s     | 11.71s   | 3.38s      | 4.06s      | 4.73s      |

## Benchmark 2: Sleep

This benchmark evaluates the performance when tasks sleep for 1 second, representing a small amount of work.
These results are with 20,000 tasks enqueued, a concurrency of `32`, and a variable number of workers.

| library  | enqueuing | 1 worker | 10 workers | 20 workers | 40 workers |
| -------- | --------- | -------- | ---------- | ---------- | ---------- |
| streaq   | 2.38s     | 626.83s  | 63.89s     | 33.12s     | 19.46s     |
| arq      | 4.82s     | 933.67s  | 173.58s    | 173.01s    | 142.53s    |
| SAQ      | 2.93s     | 642.09s  | 64.95s     | 33.66s     | 17.81s     |

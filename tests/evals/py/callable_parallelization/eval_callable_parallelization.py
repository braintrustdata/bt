import os
from threading import Condition, Lock

from braintrust import Eval
from braintrust.framework import set_thread_pool_max_workers


MAX_WORKERS = 8
TIMEOUT_SECONDS = 5
SCORE_OUT = os.environ.get("BT_PARALLEL_SCORE_OUT")

set_thread_pool_max_workers(MAX_WORKERS)


class SnapshotMaxCounter:
    def __init__(self):
        self.counter = 0
        self.cv = Condition()
        self.max_counter = self.counter

    def incr(self):
        with self.cv:
            self.counter += 1
            self.max_counter = max(self.max_counter, self.counter)
            if self.max_counter == MAX_WORKERS:
                self.cv.notify_all()

    def decr(self):
        with self.cv:
            self.counter -= 1

    def get_max_counter(self):
        with self.cv:
            return self.max_counter


score_counter = SnapshotMaxCounter()
task_counter = SnapshotMaxCounter()
score_file_lock = Lock()


def write_score(score):
    if not SCORE_OUT:
        return
    with score_file_lock:
        with open(SCORE_OUT, "a") as f:
            f.write(f"{score}\n")


def max_counter_scorer(input, output, expected):
    score_counter.incr()
    try:
        if score_counter.get_max_counter() != MAX_WORKERS:
            with score_counter.cv:
                score_counter.cv.wait(timeout=TIMEOUT_SECONDS)
        score = score_counter.get_max_counter() / MAX_WORKERS
        write_score(score)
        return score
    finally:
        score_counter.decr()


def max_counter_task(input, hooks):
    task_counter.incr()
    try:
        if task_counter.get_max_counter() != MAX_WORKERS:
            with task_counter.cv:
                task_counter.cv.wait(timeout=TIMEOUT_SECONDS)
        return task_counter.get_max_counter()
    finally:
        task_counter.decr()


def data():
    return [dict(input=i) for i in range(4)]


Eval(
    "cli-callable-parallel",
    data=data,
    task=max_counter_task,
    scores=[max_counter_scorer],
    experiment_name="parallel one",
)

Eval(
    "cli-callable-parallel",
    data=data,
    task=max_counter_task,
    scores=[max_counter_scorer],
    experiment_name="parallel two",
)

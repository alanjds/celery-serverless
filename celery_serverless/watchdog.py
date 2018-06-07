# coding: utf-8
import os
import operator
import logging
from concurrent.futures import ThreadPoolExecutor

import backoff

from celery_serverless.invoker import invoke

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


class Watchdog(object):
    def __init__(self):
        # 0) Clear counters
        self.workers_started = 0
        self.workers_fulfilled = 0
        self.executor = ThreadPoolExecutor()

    @property
    def workers_not_served(self):
        return self.workers_started - self.workers_fulfilled

    @property
    def queue_length(self):
        raise NotImplementedError()

    def trigger_workers(self, how_much:int):
        # Hack to call parameterless 'invoke' func -> lambda x: invoke
        return len([i for i in self.executor.map(lambda x: invoke, (None)*how_much)])

    @backoff.on_predicate(backoff.fibo)  # Will backoff until return True-ly val
    def _wait_starts(self, starts:int):
        return (self.workers_started >= starts)

    @backoff.on_predicate(backoff.fibo, predicate=operator.truth, max_time=20)
    def _wait_fulfillment(self):    # Stop backoff when 0 returned or max_time
        return self.workers_not_served

    def monitor(self):
        while self.queue_length:  # 1) See queue length N
            started = self.trigger_workers(self.queue_length)  # 2) Start N workers
            self._wait_starts(started)  # 3) Watch for N starts
            self._wait_fulfillment()  # 4) Wait then collect "Not Served" number

            # 5) Start "Not Served" number of workers.
            self.trigger_workers(self.workers_not_served)

        return self.workers_started  # How many had to be started to fulfill the queue?

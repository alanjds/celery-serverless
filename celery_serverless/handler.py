# coding: utf-8
import os
import signal
import json
import logging

import celery.bin.celery
from celery.signals import task_prerun

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


def _spawn_worker(softlimit:'seconds'=None, hardlimit:'seconds'=None):
    command_argv = [
        'celery',
        'worker',
        '--app project',
        '--loglevel debug',
        '--concurrency 1',
        '--prefetch-multiplier 1',
        '--without-gossip',
        '--without-mingle',
        '--task-events',
        '-O fair',
    ]

    if softlimit:
        command_argv.append('--soft-time-limit %s' % softlimit)
    if hardlimit:
        command_argv.append('--time-limit %s' % hardlimit)

    return celery.bin.celery.main(command_argv)  # will block.


def _attach_hooks():
    """
    Register the needed hooks:
    - One to store the worker class after start
    - One to trigger stop getting tasks after the 1st, and shutdown when done
    """
    @task_prerun.connect
    def _shutdown_when_done(*args, **kwargs):
        # After got a task, trigger Ctrl+C
        # The worker will stop getting tasks and will prepare to shutdown
        # but will wait the existing task to finish its lifetime.
        pid = os.getppid()  # Should hit my manager process. Not me, the minion process
        os.kill(pid, signal.SIGINT)  # Trigger Ctrl+C behaviours

    # Return the hooks to be hold somewhere. Else it could be garbage collected
    hooks = (_shutdown_when_done,)
    return hooks


def worker(event, context):
    hooks = _attach_hooks()

    remaining_seconds = context.get_remaining_time_in_millis() / 1000.0
    softlimit = remaining_seconds-30.0  # Poke the job 30sec before the abyss
    hardlimit = remaining_seconds-15.0  # Kill the job 15sec before the abyss

    _spawn_worker(
        softlimit=softlimit if softlimit > 5 else None,
        hardlimit=hardlimit if hardlimit > 5 else None,
    )  # Will block until one task got processed

    body = {
        "message": "Celery worker worked, lived, and died.",
    }

    return {
        "statusCode": 200,
        "body": json.dumps(body)
    }
    # Use this code if you don't use the http event with the LAMBDA-PROXY
    # integration
    """
    return body
    """

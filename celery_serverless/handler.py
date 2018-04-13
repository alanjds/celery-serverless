# coding: utf-8
try:  # https://github.com/UnitedIncome/serverless-python-requirements#dealing-with-lambdas-size-limitations
    import unzip_requirements
except ImportError:
    pass

import json
import logging

from celery_serverless.worker_management import spawn_worker, attach_hooks

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

hooks = []


def worker(event, context):
    global hooks

    try:
        remaining_seconds = context.get_remaining_time_in_millis() / 1000.0
    except Exception as e:
        logger.exception('Could not got remaining_seconds. Is the context right?')
        remaining_seconds = 5 * 60 # 5 minutes by default

    softlimit = remaining_seconds-30.0  # Poke the job 30sec before the abyss
    hardlimit = remaining_seconds-15.0  # Kill the job 15sec before the abyss

    logger.debug('Checking Celery hooks')
    if not hooks:
        hooks = attach_hooks()

    logger.debug('Spawning the worker(s)')
    spawn_worker(
        softlimit=softlimit if softlimit > 5 else None,
        hardlimit=hardlimit if hardlimit > 5 else None,
        loglevel='DEBUG',
    )  # Will block until one task got processed

    logger.debug('Cleaning up before exit')
    body = {
        "message": "Celery worker worked, lived, and died.",
    }

    return {"statusCode": 200, "body": json.dumps(body)}

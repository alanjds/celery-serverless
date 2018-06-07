# coding: utf-8
try:  # https://github.com/UnitedIncome/serverless-python-requirements#dealing-with-lambdas-size-limitations
    import unzip_requirements
except ImportError:
    pass

import os
import sys
import json
import logging

from .handler_utils import maybe_call_hook, import_callable, handler_wrapper, ENVVAR_NAMES

logger = logging.getLogger(__name__)
logger.propagate = True
if os.environ.get('CELERY_SERVERLESS_LOGLEVEL'):
    logger.setLevel(os.environ.get('CELERY_SERVERLESS_LOGLEVEL'))
print('Celery serverless loglevel:', logger.getEffectiveLevel())


### 1st hook call
maybe_call_hook(ENVVAR_NAMES['pre_warmup'], locals())

# Get and activate some extras
from celery_serverless.extras import discover_extras
available_extras = discover_extras()
print('Available extras:', list(available_extras.keys()), file=sys.stderr)

from celery_serverless.worker_management import spawn_worker, attach_hooks
from celery_serverless import watchdog as watchdog_module

hooks = []

### 2nd hook call
maybe_call_hook(ENVVAR_NAMES['pre_handler_definition'], locals())


@handler_wrapper(available_extras)
def worker(event, context):
    global hooks

    try:
        remaining_seconds = context.get_remaining_time_in_millis() / 1000.0
    except Exception as e:
        logger.exception('Could not got remaining_seconds. Is the context right?')
        remaining_seconds = 5 * 60 # 5 minutes by default

    softlimit = remaining_seconds-30.0  # Poke the job 30sec before the abyss
    hardlimit = remaining_seconds-15.0  # Kill the job 15sec before the abyss

    if not hooks:
        logger.debug('Fresh Celery worker. Attach hooks!')
        hooks = attach_hooks()
    else:
        logger.debug('Old Celery worker. Already have hooks.')

    # The Celery worker will start here. Will connect, take 1 (one) task,
    # process it, and quit.
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


@handler_wrapper(available_extras)
def watchdog(event, context):
    watchdog_module.monitor()

    logger.debug('Cleaning up before exit')
    body = {
        "message": "Watchdog woke, worked, and rested.",
    }
    return {"statusCode": 200, "body": json.dumps(body)}


### 3rd hook call
maybe_call_hook(ENVVAR_NAMES['post_handler_definition'], locals())

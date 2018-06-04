# coding: utf-8
try:  # https://github.com/UnitedIncome/serverless-python-requirements#dealing-with-lambdas-size-limitations
    import unzip_requirements
except ImportError:
    pass

import os
import sys
import logging

from .handler_utils import maybe_call_hook, import_callable, ENVVAR_NAMES as ENVVARS

logger = logging.getLogger(__name__)
logger.propagate = True
if os.environ.get('CELERY_SERVERLESS_LOGLEVEL'):
    logger.setLevel(os.environ.get('CELERY_SERVERLESS_LOGLEVEL'))
print('Celery serverless loglevel:', logger.getEffectiveLevel())


### 1st hook call
maybe_call_hook(ENVVARS['pre_warmup_envvar'], locals())

# Get and activate some extras
from celery_serverless.extras import discover_extras, maybe_apply_sentry
available_extras = discover_extras()
print('Available extras:', list(available_extras.keys()), file=sys.stderr)

import json
from celery_serverless.worker_management import spawn_worker, attach_hooks

hooks = []

### 2nd hook call
maybe_call_hook(ENVVARS['pre_handler_definition', locals())


@maybe_apply_sentry(available_extras)
def worker(event, context):
    global hooks

    request_id = '(unknown)'
    if 'wdb' in available_extras:
        available_extras['wdb']['start_trace']()
        # Will be True if CELERY_SERVERLESS_BREAKPOINT is defined.
        # It is the preferred way to force a breakpoint.
        if available_extras['wdb']['breakpoint']:
            import wdb
            wdb.set_trace()  # Tip: you may want to step into spawn_worker() near line 100 ;)

    try:
        ### 4th hook call
        maybe_call_hook(ENVVARS['pre_handler_call'], locals())

        try:
            request_id = context.aws_request_id
        except AttributeError:
            pass
        logger.info('START: Handle request ID: %s', request_id)

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
    except Exception as e:
        if 'sentry' in available_extras:
            logger.warning('Sending exception collected to Sentry client')
            available_extras['sentry'].captureException()

        ### Err hook call
        maybe_call_hook(ENVVARS['error_handler_call'], locals())
        raise
    finally:
        logger.info('END: Handle request ID: %s', request_id)
        ### 5th hook call
        maybe_call_hook(ENVVARS['post_handler_call'], locals())

        if 'wdb' in available_extras:
            available_extras['wdb']['stop_trace']()


### 3rd hook call
maybe_call_hook(ENVVARS['post_handler_definition'], locals())

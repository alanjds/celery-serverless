# coding: utf-8
try:  # https://github.com/UnitedIncome/serverless-python-requirements#dealing-with-lambdas-size-limitations
    import unzip_requirements
except ImportError:
    pass

import os
import importlib
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


def _maybe_call_hook(envname, locals_={}):
    func_path = os.environ.get(envname)
    logger.debug("Trying the hook %s: '%s'", envname, func_path or '(not set)')
    func = _import_callable(func_path)
    return func(locals_=locals_) if func else None


def _import_callable(name):
    result = None
    if name:
        logging.info("Importing hook '%s'", name)
        module_name, split, callable_name = name.rpartition(':')
        module = importlib.import_module(module_name)
        result = getattr(module, callable_name)
    return result if callable(result) else None


_pre_warmup_envvar = 'CELERY_SERVERLESS_PRE_WARMUP'
_pre_handler_definition_envvar = 'CELERY_SERVERLESS_PRE_HANDLER_DEFINITION'
_post_handler_definition_envvar = 'CELERY_SERVERLESS_POST_HANDLER_DEFINITION'
_pre_handler_call_envvar = 'CELERY_SERVERLESS_PRE_HANDLER_CALL'
_error_handler_call_envvar = 'CELERY_SERVERLESS_ERROR_HANDLER_CALL'
_post_handler_call_envvar = 'CELERY_SERVERLESS_POST_HANDLER_CALL'

### 1st hook call
_maybe_call_hook(_pre_warmup_envvar, locals())

import json
from celery_serverless.worker_management import spawn_worker, attach_hooks

hooks = []

### 2nd hook call
_maybe_call_hook(_pre_handler_definition_envvar, locals())


def worker(event, context):
    global hooks

    request_id = '(unknown)'
    try:
        ### 4th hook call
        _maybe_call_hook(_pre_handler_call_envvar, locals())

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
        ### Err hook call
        _maybe_call_hook(_error_handler_call_envvar, locals())
        raise
    finally:
        logger.info('END: Handle request ID: %s', request_id)
        ### 5th hook call
        _maybe_call_hook(_post_handler_call_envvar, locals())


### 3rd hook call
_maybe_call_hook(_post_handler_definition_envvar, locals())

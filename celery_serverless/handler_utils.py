import os
import functools
import importlib
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.propagate = True
if os.environ.get('CELERY_SERVERLESS_LOGLEVEL'):
    logger.setLevel(os.environ.get('CELERY_SERVERLESS_LOGLEVEL'))
print('Celery serverless loglevel:', logger.getEffectiveLevel())

from celery_serverless.extras import maybe_apply_sentry


ENVVAR_NAMES = {
    'pre_warmup': 'CELERY_SERVERLESS_PRE_WARMUP',
    'pre_handler_definition': 'CELERY_SERVERLESS_PRE_HANDLER_DEFINITION',
    'post_handler_definition': 'CELERY_SERVERLESS_POST_HANDLER_DEFINITION',
    'pre_handler_call': 'CELERY_SERVERLESS_PRE_HANDLER_CALL',
    'error_handler_call': 'CELERY_SERVERLESS_ERROR_HANDLER_CALL',
    'post_handler_call': 'CELERY_SERVERLESS_POST_HANDLER_CALL',
}


def maybe_call_hook(envname, locals_={}):
    func_path = os.environ.get(envname)
    logger.debug("Trying the hook %s: '%s'", envname, func_path or '(not set)')
    func = import_callable(func_path)
    return func(locals_=locals_) if func else None


def import_callable(name):
    result = None
    if name:
        logging.info("Importing hook '%s'", name)
        module_name, split, callable_name = name.rpartition(':')
        module = importlib.import_module(module_name)
        result = getattr(module, callable_name)
    return result if callable(result) else None


def handler_wrapper(fn, available_extras):
    @maybe_apply_sentry(available_extras)
    @functools.wraps(fn)
    def _handler(event, context):
        request_id = '(unknown)'
        try:
            if 'wdb' in available_extras:
                available_extras['wdb']['start_trace']()
                # Will be True if CELERY_SERVERLESS_BREAKPOINT is defined.
                # It is the preferred way to force a breakpoint.
                if available_extras['wdb']['breakpoint']:
                    import wdb
                    wdb.set_trace()  # Tip: you may want to step into fn() near line 72 ;)

            ### 4th hook call
            maybe_call_hook(ENVVAR_NAMES['pre_handler_call'], locals())

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

            result = fn(event, context)
            logger.debug('Cleaning up before exit')
            return result
        except Exception as e:
            if 'sentry' in available_extras:
                logger.warning('Sending exception collected to Sentry client')
                available_extras['sentry'].captureException()

            ### Err hook call
            maybe_call_hook(ENVVAR_NAMES['error_handler_call'], locals())
            raise
        finally:
            logger.info('END: Handle request ID: %s', request_id)
            ### 5th hook call
            maybe_call_hook(ENVVAR_NAMES['post_handler_call'], locals())

            if 'wdb' in available_extras:
                available_extras['wdb']['stop_trace']()
    return _handler

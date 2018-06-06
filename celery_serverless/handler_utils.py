import os
import functools
import importlib
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.propagate = True
if os.environ.get('CELERY_SERVERLESS_LOGLEVEL'):
    logger.setLevel(os.environ.get('CELERY_SERVERLESS_LOGLEVEL'))
print('Celery serverless handlers loglevel:', logger.getEffectiveLevel())

from celery_serverless.extras import discover_extras, maybe_apply_sentry


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


def handler_wrapper(fn):
    available_extras = discover_extras()

    @functools.wraps(fn)
    @maybe_apply_sentry(available_extras)
    def _handler(event, context):
        request_id = '(unknown)'
        try:
            if 'wdb' in available_extras:
                available_extras['wdb']['start_trace']()
                # Will be True if CELERY_SERVERLESS_BREAKPOINT is defined.
                # It is the preferred way to force a breakpoint.
                if available_extras['wdb']['breakpoint']:
                    import wdb
                    wdb.set_trace()  # Tip: you may want to step into fn() near line 70 ;)

            ### 4th hook call
            maybe_call_hook(ENVVAR_NAMES['pre_handler_call'], locals())

            try:
                request_id = context.aws_request_id
            except AttributeError:
                pass
            logger.info('START: Handle request ID: %s', request_id)

            return fn(event, context)
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

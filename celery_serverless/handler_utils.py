import os
import sys
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


_called_hooks = set()
def _had_already_ran(hookname) -> bool:
    """
    Returns False if is the 1st run with  this 'hookname'. True otherwise.
    """
    if hookname in _called_hooks:
        return True
    else:
        _called_hooks.add(hookname)
        return False


def _warmup_hooks(locals_={}):
    if _had_already_ran('warmup'):
        return

    ### 1st hook call
    _maybe_call_hook(ENVVAR_NAMES['pre_warmup'], locals_)

    available_extras = discover_extras(apply_s3conf=True)
    print('Available extras:', list(available_extras.keys()), file=sys.stderr)
    locals_['available_extras'] = available_extras

    ### 2nd hook call
    _maybe_call_hook(ENVVAR_NAMES['pre_handler_definition'], locals_)


def _post_handler_definition_hook(locals_={}):
    if _had_already_ran('post_handler_definition'):
        return

    ### 3rd hook call
    _maybe_call_hook(ENVVAR_NAMES['post_handler_definition'], locals_)


def handler_wrapper(fn):
    ### 1st and 2nd hook calls
    _warmup_hooks(locals_=globals())  # Do sets 'available_extras' global

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
            _maybe_call_hook(ENVVAR_NAMES['pre_handler_call'], locals())

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
            _maybe_call_hook(ENVVAR_NAMES['error_handler_call'], locals())
            raise
        finally:
            logger.info('END: Handle request ID: %s', request_id)
            ### 5th hook call
            _maybe_call_hook(ENVVAR_NAMES['post_handler_call'], locals())

            if 'wdb' in available_extras:
                available_extras['wdb']['stop_trace']()

    ### 3rd hook call
    _post_handler_definition_hook(locals_=globals())

    return _handler

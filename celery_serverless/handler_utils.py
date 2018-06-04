import os
import logging
import importlib

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.propagate = True
if os.environ.get('CELERY_SERVERLESS_LOGLEVEL'):
    logger.setLevel(os.environ.get('CELERY_SERVERLESS_LOGLEVEL'))
print('Celery serverless loglevel:', logger.getEffectiveLevel())


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

# coding: utf-8
import os
import functools
import logging

logger = logging.getLogger(__name__)

available_extras = {}


## Discoverables:

def discover_sentry():
    ## Sentry extras:
    SENTRY_DSN = os.environ.get('SENTRY_DSN')
    if SENTRY_DSN and not os.environ.get('CELERY_SERVERLESS_NO_SENTRY'):
        logger.info('Activating Sentry extra support')
        try:
            import raven
        except ImportError:
            raise RuntimeError("Could not import 'raven'. Have you installed the the ['sentry'] extra?")
        from celery_serverless.extras.sentry import get_sentry_client
        return {'sentry': get_sentry_client()}
    return {}


def discover_logdrain():
    ## Lodrain extras:
    LOGDRAIN_URL = os.environ.get('LOGDRAIN_URL')
    if LOGDRAIN_URL and not os.environ.get('CELERY_SERVERLESS_NO_LOGDRAIN'):
        logger.info('Activating Logdrain extra support')
        try:
            import raven
        except ImportError:
            raise RuntimeError("Could not import 'raven'. Have you installed the the ['logdrain'] extra?")
        from celery_serverless.extras.logdrain import init_logdrain
        return {'logdrain': init_logdrain()}
    return {}


def discover_wdb():
    ## Web Debugger extras:
    WDB_SOCKET_SERVER = os.environ.get('WDB_SOCKET_SERVER')
    WDB_SOCKET_PORT = os.environ.get('WDB_SOCKET_PORT')
    WDB_SOCKET_URL = os.environ.get('WDB_SOCKET_URL')

    needed_envs_available = bool(WDB_SOCKET_URL or (WDB_SOCKET_SERVER and WDB_SOCKET_PORT))
    if needed_envs_available and not os.environ.get('CELERY_SERVERLESS_NO_WDB'):
        logger.info('Activating WDB (Web Debugger) extra support')
        try:
            import wdb
        except ImportError:
            raise RuntimeError("Could not import 'wdb'. Have you installed the the ['wdb'] extra?")
        from celery_serverless.extras.wdb import init_wdb
        return {'wdb': init_wdb()}
    return {}


def discover_s3conf():
    ## S3Conf extras:
    S3CONF = os.environ.get('S3CONF')
    if S3CONF and not os.environ.get('CELERY_SERVERLESS_NO_S3CONF'):
        logger.info('Activating S3CONF extra support')
        try:
            import s3conf
        except ImportError:
            raise RuntimeError("Could not import 's3conf'. Have you installed the the ['s3conf'] extra?")
        from celery_serverless.extras.s3conf import init_s3conf
        return {
            's3conf': {
                'apply':init_s3conf,
            }
        }
    return {}

    
## Discoverer

## Discoverer

DISCOVER_FUNCTIONS = [
    discover_s3conf,  # Will be handled directly, as changes other extras availability
    discover_sentry,
    discover_logdrain,
    discover_wdb,
]

def discover_extras(apply_s3conf=False):
    # S3CONF is special. It is slower and change the others. Save it for later.
    if 's3conf' in available_extras:
        s3conf_extra = {'s3conf': available_extras['s3conf']}
    else:
        s3conf_extra = {}

    # Get and activate some extras, starting by environment-related ones
    if apply_s3conf:
        if not s3conf_extra:
            s3conf_extra = discover_s3conf()   # {} or {'s3conf': ...}

        if s3conf_extra.get('s3conf', {}).get('apply'):  # Available and not applyed
            # Should be the 1st one called because can set the environment
            # Then the next ones can be discovered based on s3conf results
            logger.debug('Applying S3CONF serverless environment extra')
            s3conf_extra = {'s3conf': s3conf_extra['s3conf'].pop('apply')()}

    available_extras.clear()
    for func in DISCOVER_FUNCTIONS:
        if func is discover_s3conf and s3conf_extra:  # Reuse slow task if possible
            func = lambda: s3conf_extra
        available_extras.update(func())
    return available_extras


## Helpers

def maybe_apply_sentry(available_extras):
    if callable(available_extras):
        raise TypeError("Should initialize the decorator with 'available_extras' map, not %s", type(available_extras))

    def _decorator(fn):
        if 'sentry' in available_extras:
            logger.debug('Applying Sentry serverless handler wrapper extra')
            fn = available_extras['sentry'].capture_exceptions(fn)
        return fn
    return _decorator

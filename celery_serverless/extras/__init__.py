# coding: utf-8
import os
import logging

logger = logging.getLogger(__name__)

available_extras = set()


def discover_sentry():
    ## Sentry extras:
    SENTRY_DSN = os.environ.get('SENTRY_DSN')
    if SENTRY_DSN and not os.environ.get('CELERY_SERVERLESS_NO_SENTRY'):
        logger.info('Activating Sentry extra support')
        try:
            import raven
        except ImportError:
            raise RuntimeError("Could not import 'raven'. Have you installed the the ['sentry'] extra?")
        return ['sentry']


def discover_logdrain():
    ## Lodrain extras:
    LOGDRAIN_URL = os.environ.get('LOGDRAIN_URL')
    if LOGDRAIN_URL and not os.environ.get('CELERY_SERVERLESS_NO_LOGDRAIN'):
        logger.info('Activating Logdrain extra support')
        return ['logdrain']


DISCOVER_FUNCTIONS = [discover_sentry, discover_logdrain]

def discover_extras():
    available_extras.clear()
    for func in DISCOVER_FUNCTIONS:
        found = func()
        if found:
            available_extras.union(found)
    return available_extras

# Initialize the extras registry
discover_extras()

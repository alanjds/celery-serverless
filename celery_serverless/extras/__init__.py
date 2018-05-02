# coding: utf-8
import os
import logging

logger = logging.getLogger(__name__)

available_extras = {}


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
        from celery_serverless.extras.logdrain import init_logdrain
        return {'logdrain': init_logdrain()}
    return {}


DISCOVER_FUNCTIONS = [discover_sentry, discover_logdrain]

def discover_extras():
    available_extras.clear()
    for func in DISCOVER_FUNCTIONS:
        available_extras.update(func())
    return available_extras

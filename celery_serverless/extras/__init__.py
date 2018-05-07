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


def discover_wdb():
    ## Web Debugger extras:
    WDB_SOCKET_SERVER = os.environ.get('WDB_SOCKET_SERVER')
    WDB_SOCKET_PORT = os.environ.get('WDB_SOCKET_PORT')
    WDB_SOCKET_URL = os.environ.get('WDB_SOCKET_URL')
    if WDB_SOCKET_URL:
        try:
            WDB_SOCKET_URL, WDB_SOCKET_PORT = WDB_SOCKET_URL.rsplit(':')
        except ValueError as err:
            msg = str(err) + '. WDB_SOCKET_SERVER format should be "tcp://hostname:port"'
            raise ValueError(msg) from err
        WDB_SOCKET_URL = os.environ.setdefault('WDB_SOCKET_URL', WDB_SOCKET_URL)
        WDB_SOCKET_PORT = os.environ.setdefault('WDB_SOCKET_PORT', WDB_SOCKET_PORT)

    if WDB_SOCKET_SERVER and WDB_SOCKET_PORT and not os.environ.get('CELERY_SERVERLESS_NO_WDB'):
        logger.info('Activating WDB (Web Debugger) extra support')
        try:
            import wdb
        except ImportError:
            raise RuntimeError("Could not import 'wdb'. Have you installed the the ['wdb'] extra?")
        from celery_serverless.extras.wdb import init_wdb
        return {'wdb': init_wdb()}
    return {}


DISCOVER_FUNCTIONS = [discover_sentry, discover_logdrain, discover_wdb]

def discover_extras():
    available_extras.clear()
    for func in DISCOVER_FUNCTIONS:
        available_extras.update(func())
    return available_extras

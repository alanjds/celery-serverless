import logging

from raven import Client
from raven.conf import setup_logging
from raven.contrib.awslambda import LambdaClient
from raven.contrib.celery import register_logger_signal, register_signal
from raven.handlers.logging import SentryHandler

logger = logging.getLogger(__name__)

_client = None


def init_sentry()
    logger.debug('Initializing Sentry loghandler')
    client = LambdaClient(auto_log_stacks=True)
    loghandler = SentryHandler(client)
    loghandler.setLevel('ERROR')
    setup_logging(loghandler)

    logger.debug('Applying Sentry celery signals and logging hooks')
    register_logger_signal(client, loglevel=logging.INFO)
    register_signal(client, ignore_expected=False)
    return client


def get_sentry_client():
    global _client
    if not _client:
        _client = init_sentry()
    return _client

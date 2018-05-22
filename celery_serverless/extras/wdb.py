import logging
import os
from urllib.parse import urlparse

import wdb
from wdb import start_trace, stop_trace

logger = logging.getLogger(__name__)


def init_wdb():
    """
    Set the environment up, if provided a WDB_SOCKET_URL in the format:
        `tcp://serverhostname:port`
    """
    logger.debug('Initializing WDB support envvars')
    os.environ['WDB_NO_BROWSER_AUTO_OPEN'] = 'True'
    WDB_SOCKET_URL = os.environ.get('WDB_SOCKET_URL')
    CELERY_SERVERLESS_BREAKPOINT = os.environ.get('CELERY_SERVERLESS_BREAKPOINT')

    if WDB_SOCKET_URL:
        parsed = urlparse(WDB_SOCKET_URL)
        if parsed.scheme.partition('+')[0] != 'tcp':
            raise ValueError('WDB_SOCKET_URL format should be "tcp://serverhostname:port"')

        # I should not need to do it. Anyway is how it works...
        wdb.SOCKET_SERVER = os.environ.setdefault('WDB_SOCKET_SERVER', str(parsed.hostname or ''))
        wdb.SOCKET_PORT = int(os.environ.setdefault('WDB_SOCKET_PORT', str(parsed.port or '')))

    logger.debug(
        'Using WDB_SOCKET_SERVER=%s WDB_SOCKET_PORT=%s',
        os.environ['WDB_SOCKET_SERVER'],
        os.environ['WDB_SOCKET_PORT'],
    )

    # See: https://github.com/Kozea/wdb/pull/98#issue-130738238
    # importmagic.index spams the console on every new import. Is annoying,
    # but on Lambdas it can cost a lot of money too.
    logger.debug("Lowering the loglevel of 'importmagic.index'")
    importmagic_logger = logging.getLogger('importmagic.index')
    importmagic_logger.setLevel('ERROR')
    importmagic_logger.propagate = False

    return {'start_trace': start_trace, 'stop_trace': stop_trace, 'breakpoint': CELERY_SERVERLESS_BREAKPOINT}

import logging
import os
from urllib.parse import urlparse

from wdb import start_trace, stop_trace

logger = logging.getLogger(__name__)


def init_wdb():
    """
    Set the environment up, if provided a WDB_SOCKET_URL in the format:
        `tcp://serverhostname:port`
    """
    logger.debug('Initializing WDB support envvars')
    os.environ['WDB_NO_BROWSER_AUTO_OPEN'] = True
    WDB_SOCKET_URL = os.environ.get('WDB_SOCKET_URL')

    if WDB_SOCKET_URL:
        parsed = urlparse(WDB_SOCKET_URL)
        if parsed.scheme.partition('+')[0] != 'tcp':
            raise ValueError('WDB_SOCKET_URL format should be "tcp://serverhostname:port"')

        os.environ.setdefault('WDB_SOCKET_SERVER', parsed.hostname)
        os.environ.setdefault('WDB_SOCKET_PORT', parsed.port)

    return {'start_trace': start_trace, 'stop_trace': stop_trace}

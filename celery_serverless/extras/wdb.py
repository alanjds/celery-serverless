import logging
import os

from wdb import start_trace, stop_trace

logger = logging.getLogger(__name__)


def init_wdb():
    logger.debug('Initializing WDB support envvars')
    os.environ['WDB_NO_BROWSER_AUTO_OPEN'] = True
    return {'start_trace': start_trace, 'stop_trace': stop_trace}

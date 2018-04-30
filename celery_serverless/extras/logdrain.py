import os
import sys
import socket
import logging
from urllib.parse import urlparse

from raven.conf import setup_logging

logger = logging.getLogger(__name__)

logdrain_url = os.environ.get('LOGDRAIN_URL')
logdrain_logformat = os.environ.get('LOGDRAIN_LOGFORMAT')


def get_syslog_handler(logdrain_url:str):
    """
    Builds a SysLogHandler from a Syslog LOGDRAIN_URL in the format:
        `syslog[+udp|+tcp]://serverhostname[:port][#LOGLEVEL]`
    """
    from logging.handlers import SysLogHandler

    parsed = urlparse(logdrain_url)
    if not parsed.scheme.partition('+')[0] == 'syslog':
        raise ValueError('LOGDRAIN_URL provided wrong or unsuported scheme "%s"', parsed.scheme)
    
    logger.debug('Syslog logdrain detected')

    basescheme, _, wrap = parsed.scheme.partition('+')
    wrap = wrap or 'udp'    # Syslog defaults to UDP
    hostname = parsed.hostname
    port = parsed.port

    if not port:
        if wrap == 'tls':
            port = 6514
        elif warp == 'tcp':
            port = 601
        elif wrap == 'udp':
            port = 514
        else:
            raise ValueError("LOGDRAIN_URL provided no 'port' and an unknown/unsuported 'scheme'")

    if wrap == 'tls':
        raise NotImplementedError('Is not (yet) possible to drain to Syslog+TLS locations. A pull-request is welcome ;) ')
    elif wrap == 'tcp':
        socktype = socket.SOCK_STREAM
    elif wrap == 'udp':
        socktype = socket.SOCK_DGRAM
    else:
        raise ValueError('Connection wrap "%s" is unknown or unsupported. Please use UDP or TCP for now' % wrap.upper())

    handler = SysLogHandler(address=(hostname, port), socktype=socktype)
    if parsed.fragment:
        handler.setLevel(parsed.fragment)
    return handler


def init_logdrain(logdrain_url=logdrain_url, logdrain_logformat=logdrain_logformat):
    if logdrain_url.startswith('syslog'):
        handler = get_syslog_handler(logdrain_url)
    else:
        raise NotImplementedError('Could not initialize LOGDRAIN_URL "%s". Only "syslog" is supported for now' % logdrain_url)

    if logdrain_logformat:
        formatter = logging.Formatter(logdrain_logformat, "%Y-%m-%dT%H:%M:%SZ")
        handler.setFormatter(formatter)

    setup_logging(handler, exclude=[])  # Sentry made it so easy! Thanks S2
    return handler

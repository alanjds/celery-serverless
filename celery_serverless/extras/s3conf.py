import logging
import os

from s3conf.s3conf import S3Conf

logger = logging.getLogger(__name__)


def init_s3conf():
    """
    Set the environment up, if provided a S3CONF in the format:
        `s3://bucketname/filepath/with/folders/envfile.env`
    """
    logger.debug('Initializing S3CONF environment')
    conf = S3Conf()
    external_environ = conf.get_envfile().as_dict()
    os.environ.update(external_environ)

    S3CONF_MAP = os.environ.get('S3CONF_MAP')
    if S3CONF_MAP:
        logger.debug('Fetching S3CONF_MAP environment files')
        conf.downsync(S3CONF_MAP)

    return {
        'client': conf,
        'external_environ': external_environ,
        's3conf_map': S3CONF_MAP,
    }

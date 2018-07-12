# coding: utf-8
import os
import asyncio
from threading import Thread
from inspect import isawaitable
from urllib.parse import urlparse
import importlib

try:
    from redis import StrictRedis

    # MONKEYPATCH until https://github.com/andymccurdy/redis-py/pull/1007/ is merged
    import redis.lock
    def __locked(self):
        if self.redis.get(self.name) is not None:
            return True
        return False
    if not hasattr(redis.lock.Lock, 'locked'):
        redis.lock.Lock.locked = __locked
except ImportError:
    pass

slave_loop = None    # loop
slave_thread = None  # type: Thread


def _start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def _start_thread():
    global slave_loop
    global slave_thread
    slave_loop = slave_loop or asyncio.new_event_loop()
    slave_thread = slave_thread or Thread(target=_start_loop, args=(slave_loop,))
    slave_thread.daemon = True   # Clean itself on Python exit.
    slave_thread.start()


def run_aio_on_thread(coro):
    if not isawaitable(coro):
        raise TypeError('An awaitable should be provided')

    if not slave_thread:
        _start_thread()
    return asyncio.run_coroutine_threadsafe(coro, slave_loop)


def _get_lock(lock_url='', lock_url_env='CELERY_SERVERLESS_LOCK_URL', lock_url_default='dummy_threading://',
              lock_name='', lock_name_env='CELERY_SERVERLESS_LOCK_NAME', lock_name_default='',
              enforce=True) -> '(Lock, str)':
    """
    Build or fetch a lock from `lock_url` or `lock_url_env` envvar.
    If needed, use `lock_url` or `lock_name_env` envvar for the lock name,
    falling back to `lock_name_default` contents.
    Pass `enforce=True` raise a RuntimeError if lock url got empty.
    """
    lock_name = os.environ.get(lock_name_env, lock_name_default)
    lock_url = os.environ.get(lock_url_env, '')
    if enforce:
        if lock_url == 'disabled':
            lock_url = ''
        else:
            assert lock_url, ('The %s envvar should be set. Even to "disabled" to disable it.' % lock_url_env)

    if lock_url.startswith(('redis://', 'rediss://')):
        redis = StrictRedis.from_url(lock_url)
        lock = redis.lock(lock_name)
    elif lock_url_default and not lock_url:
        defaultlock_module_name = urlparse(lock_url_default).scheme
        defaultlock_module = importlib.import_module(defaultlock_module_name)
        lock = defaultlock_module.Lock()
    else:
        raise RuntimeError("This URL is not supported. Only 'redis[s]://...' is supported for now")
    return lock, lock_name


def get_watchdog_lock(enforce=True) -> '(Lock, str)':
    return _get_lock(lock_url_env='CELERY_SERVERLESS_LOCK_URL', lock_url_default='dummy_threading://',
                     lock_name_env='CELERY_SERVERLESS_LOCK_NAME', lock_name_default='celery_serverless:watchdog',
                     enforce=enforce)


_CLIENT_LOCK = {}

def get_client_lock(enforce=False) -> '(Lock, str)':
    if _CLIENT_LOCK:
        return _CLIENT_LOCK['lock'], _CLIENT_LOCK['lock_name']

    lock, lock_name = _get_lock(
        lock_url_env='CELERY_SERVERLESS_LOCK_URL', lock_url_default='multiprocessing://',
        lock_name_env='CELERY_SERVERLESS_LOCK_NAME', lock_name_default='celery_serverless:watchdog-client',
        enforce=enforce,
    )

    _CLIENT_LOCK.update({'lock': lock, 'lock_name': lock_name})
    return _CLIENT_LOCK['lock'], _CLIENT_LOCK['lock_name']

# coding: utf-8
import os
import signal
import logging
from functools import partial

import celery.bin.celery
from celery.signals import celeryd_init, worker_ready

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


def spawn_worker(softlimit:'seconds'=None, hardlimit:'seconds'=None, **options):
    command_argv = [
        'celery',
        'worker',
        # '--app', 'project',
        '--concurrency', '1',
        '--prefetch-multiplier', '1',
        '--without-gossip',
        '--without-mingle',
        '--task-events',
        '-O', 'fair',
    ]

    if softlimit:
        command_argv.extend(['--soft-time-limit', '%s' % softlimit])
    if hardlimit:
        command_argv.extend(['--time-limit', '%s' % hardlimit])

    for k, v in options.items():
        if len(k) == 1:
            option = '-%s' % k
        else:
            option = '--%s' % k
        command_argv.append(option)

        if v:
            command_argv.append('%s' % v)

    logger.info('Starting the worker(s)')
    logger.debug('Command: %s', ' '.join(command_argv))
    try:
        celery.bin.celery.main(command_argv)  # Will block until worker dies.
    except SystemExit as e:  # Worker is dead.
        return e
    raise RuntimeError('Is the worker left running?')


def shutdown_when_done(*args, **kwargs):
    # Start to shutdown by triggering <Ctrl+C>
    # The worker will stop getting tasks and will prepare to shutdown
    # but will wait the existing task to finish its lifetime.
    logger.debug('Informing the workers to stop accepting tasks')
    pid = os.getpid()
    os.kill(pid, signal.SIGINT)  # Trigger Ctrl+C behaviours


def wakeme_soon(callback:'callable'=None, delay:'seconds'=1.0, reason='', *args, **kwargs):
    # After born, wait some seconds and get a UNIX signal poke.
    if reason:
        reason = 'waiting for "%s"' % reason
    logger.debug('Registering an ALRM for %.2f seconds ahead %s', delay, reason)
    signal.signal(signal.SIGALRM, callback)
    signal.setitimer(signal.ITIMER_REAL, delay)


def attach_hooks(wait_connection=4.0, wait_job=1.0):
    """
    Register the needed hooks:
    - One to trigger stop getting tasks after the 1st, and shutdown when done
    """
    logger.info('Attaching hooks')
    logger.debug('Wait connection time: %.2f', wait_connection)
    logger.debug('Wait job time: %.2f', wait_job)

    @celeryd_init.connect  # After worker process up
    def _broker_connection_timeout(*args, **kwargs):
        return wakeme_soon(reason='broker connection', delay=wait_connection, callback=shutdown_when_done)

    @worker_ready.connect  # After broker queue connected
    def _worker_ready_timeout(*args, **kwargs):
        return wakeme_soon(reason='job to come', delay=wait_job, callback=shutdown_when_done)

    return [_broker_connection_timeout, _worker_ready_timeout]  # Using weak references

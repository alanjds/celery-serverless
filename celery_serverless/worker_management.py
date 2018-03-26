# coding: utf-8
import os
import signal
import logging

import celery.bin.celery
from celery.signals import task_prerun

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


def spawn_worker(softlimit:'seconds'=None, hardlimit:'seconds'=None, **options):
    command_argv = [
        'celery',
        'worker',
        '--app', 'project',
        '--loglevel', 'debug',
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
    # After got a task, trigger Ctrl+C
    # The worker will stop getting tasks and will prepare to shutdown
    # but will wait the existing task to finish its lifetime.
    logger.debug('Informing the worker to stop accepting tasks')
    pid = os.getppid()  # Should hit my manager process. Not me, the minion process
    os.kill(pid, signal.SIGINT)  # Trigger Ctrl+C behaviours


def attach_hooks():
    """
    Register the needed hooks:
    - One to trigger stop getting tasks after the 1st, and shutdown when done
    """
    logger.debug('Attaching hooks')

    # Return the hooks to be hold somewhere. Else it could be garbage collected
    hooks = []
    hooks.append(task_prerun.connect(shutdown_when_done))
    return hooks

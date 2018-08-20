# coding: utf-8
import os
import uuid
import signal
import logging
from datetime import timedelta, datetime

import celery.bin.celery
import celery.worker.state
import celery.worker.request
from celery.signals import celeryd_init, worker_ready, task_prerun, task_postrun, task_success
from celery.exceptions import WorkerShutdown

from celery_serverless import watchdog

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


class PatchedRequest(celery.worker.request.Request):
    def __init__(self, *args, **kwargs):
        res = super(__class__, self).__init__(*args, **kwargs)
        logger.warning('Passing Request into the tasks')
        self.task._original_request = self  # Should I ?
# Monkey patch!! Ahoy!
celery.worker.request._patched_Request = celery.worker.request.Request
celery.worker.request.Request = PatchedRequest


def _get_options_from_environ():
    """Gets all CELERY_WORKER_* environment vars"""
    for k,v in os.environ.items():
        if k.upper().startswith('CELERY_WORKER_'):
            k = k.lower().partition('celery_worker_')[-1]
            yield (k,v)


def wakeme_soon(callback:'callable'=None, delay:'seconds'=1.0, reason='', *args, **kwargs):
    """
    Sets an alarm via Unix SIGALRM up to 'seconds' ahead. Then calls the 'callback'.
    Beware that ONLY ONE alarm can exist at a time. This is how Unix works.
    If this function is called multiple times, only the last call remains active.
    """
    if reason:
        reason = 'waiting for "%s"' % reason
    logger.debug('Registering an ALRM for %.2f seconds ahead %s', delay, reason)
    signal.signal(signal.SIGALRM, callback)
    signal.setitimer(signal.ITIMER_REAL, delay)


def cancel_wakeme():
    """Disables the actual Unix SIGALRM set, if any"""
    signal.signal(signal.SIGALRM, signal.SIG_DFL)   # Play safe
    signal.setitimer(signal.ITIMER_REAL, 0)  # Disables the timer


def remaining_lifetime_getter(lambda_context=None) -> 'float':
    """
    Generates the remaining lifetime of this Lambda in seconds,
    given a 'lambda_context'
    """
    starting_time = datetime.now()
    try:
        initial_remaining_millis = lambda_context.get_remaining_time_in_millis()
    except Exception as e:
        logger.exception('Could not get remaining_seconds. Is the context right?')
        initial_remaining_millis = 5 * 60 * 1000 # 5 minutes by default
    ending_time = starting_time + timedelta(milliseconds=initial_remaining_millis)

    while True:
        remaining_seconds = (ending_time - datetime.now()).seconds
        logger.info('Remaining time calculated: %s sec.', remaining_seconds)
        yield remaining_seconds


class WorkerRunner(object):
    # To store the Worker instance.
    # Sometime it will change to a Thread or Async aware thing
    worker = None

    def __init__(self, task_max_lifetime=300-15-30, softlimit=30, hardlimit=15):
        self._softlimit = softlimit
        self._hardlimit = hardlimit
        self._task_max_lifetime = task_max_lifetime
        self._intercom_url = os.environ.get('CELERY_SERVERLESS_INTERCOM_URL')

    def run_worker(self, **options):
        remaining_seconds = next(self._lifetime_getter)

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
            '-P', 'solo',
        ]

        softlimit = remaining_seconds-self._softlimit  # Poke the job 30sec before the abyss
        if softlimit > 5:  # At least 5 sec.
            command_argv.extend(['--soft-time-limit', '%s' % softlimit])

        hardlimit = remaining_seconds-self._hardlimit  # Kill the job 15sec before the abyss
        if hardlimit:  # At least 5 sec.
            command_argv.extend(['--time-limit', '%s' % hardlimit])

        options.update(dict(_get_options_from_environ()))
        for k, v in options.items():
            if len(k) == 1:
                option = '-%s' % k.lower()
            else:
                option = '--%s' % k.lower()
            command_argv.append(option)

            if v:
                command_argv.append('%s' % v)

        logger.info('Starting the worker(s)')
        logger.debug('Command: %s', ' '.join(command_argv))
        try:
            celery.bin.celery.main(command_argv)  # Will block until worker dies.
        except SystemExit as e:  # Worker is dead.
            logger.debug('Caught a SystemExit.')
            state = celery.worker.state
            state.should_stop = False
            state.should_terminate = False
            return e
        raise RuntimeError('Is the worker left running?')

    def _shutdown_worker(self):
        # Inform the Watchdog Monitor [leave]
        watchdog_context = self._watchdog_context
        watchdog.inform_worker_leave(
            watchdog_context['intercom'],
            watchdog_context['worker_id'],
            prefix=watchdog_context['prefix'],
        )
        raise WorkerShutdown()

    def is_time_up(self):
        """
        Will a new task fail if it uses the whole task_max_lifetime?
        """
        return self._task_max_lifetime > self._lifetime_getter()

    def set_worker_metadata(self, intercom_url='', worker_metadata=None):
        intercom_url = intercom_url or self._intercom_url
        assert intercom_url, 'The CELERY_SERVERLESS_INTERCOM_URL envvar should be set. Even to "disabled" to disable it.'

        worker_metadata = worker_metadata or {
            'worker_id': uuid.uuid1(),
            'prefix': '(undefined)',
        }

        self._watchdog_context = {
            'intercom': watchdog.build_intercom(intercom_url),
            'worker_metadata': worker_metadata,
            'worker_id': worker_metadata['worker_id'],
            'prefix': worker_metadata['prefix'],
        }
        logger.debug("Event -> _watchdog_context: %s", self._watchdog_context)

    def attach_hooks(self, wait_connection=8.0, wait_job=4.0):
        """
        Register the needed hooks:
        - At start, shutdown if cannot get a Broker within 'wait_connection' seconds
        - After broker connected, shutdown if cannot receive a job within 'wait_job'
        This safeguards against empty queues too.
        - Receiving a job, clears the watchdogs.
        - Finished a job, set an alarm for shutdown, allowing Task to acknowledge()
        - If a new task comes after the 1st processed, reject and shutdown.
        """
        logger.info('Attaching Celery hooks')
        logger.debug('Wait connection time: %.2f', wait_connection)
        logger.debug('Wait job time: %.2f', wait_job)

        @celeryd_init.connect  # After worker process up
        def _set_broker_watchdog(conf=None, instance=None, *args, **kwargs):
            logger.debug('Connecting to the broker [celeryd_init]')
            self._worker = worker = instance

            worker.__broker_connected = False
            def _maybe_shutdown(*args, **kwargs):
                assert worker.__broker_connected == False, 'Broker conected but ALRM received?'
                logger.info('Shutting down. Never connected to the broker [callback:celeryd_init]')
                self._shutdown_worker()

            # Set shutdown signal in case we don't connect in wait_connection seconds
            wakeme_soon(delay=wait_connection, callback=_maybe_shutdown)

        # #######
        # @worker_init.connect  # Before connecting, if -P solo
        # def _worker_init(sender=None, *args, **kwargs):
        #     logger.debug('Connecting to the broker [worker_init]')
        # #######

        @worker_ready.connect  # After broker queue connected
        def _set_job_watchdog(sender=None, *args, **kwargs):
            # assert context['worker'] == sender.controller, 'Oops: Are the CONTEXT messed?'
            worker = self._worker
            worker.__broker_connected = True
            logger.debug('Connected to the broker! [worker_ready]')

            worker.__task_received = False
            worker.__task_finished = False

            # HACK: (Re)start to listen the queue. Could had been silenced before.
            worker.consumer.add_task_queue('celery')  # TODO: Select the queue dynamically

            def _maybe_shutdown(*args, **kwargs):
                if worker.__task_received:
                    logger.debug('Keep going. Task received [callback:worker_ready]')
                else:
                    logger.info('Shutting down. Never received a Task [callback:worker_ready]')
                    self._shutdown_worker()
            # only one alarm SIGALRM is allowed to exist at a time
            # this cancels any previous alarms and set a new one
            wakeme_soon(delay=wait_job, callback=_maybe_shutdown)

        @task_prerun.connect  # Task already got.
        def _unset_watchdogs(*args, **kwargs):
            # Worker is not received on this signal, direct or indirectly :/
            worker = self._worker
            worker.__task_received = True

            logger.info('Task received! [task_prerun]')
            cancel_wakeme()

            # New task should be rejected and returned if one was already processed.
            worker.consumer.connection._default_channel.do_restore = True
            assert not worker.__task_finished, 'Should had shutdown on task_success. Not here!'

            watchdog_context = self._watchdog_context
            watchdog.inform_worker_busy(
                watchdog_context['intercom'],
                watchdog_context['worker_id'],
                prefix=watchdog_context['prefix'],
            )

        @task_success.connect
        def _ack_success(sender=None, *args, **kwargs):
            task = sender
            logger.info('Job done. Stop receiving new messages avoiding Kombu prefetch. [task_success]')

            worker = self._worker  # TODO: Get the list of queues.
            worker.consumer.cancel_task_queue('celery')  # Manually, to prevent the next line to receive a new task
            task._original_request.message.ack()  # Manually, as WorkerShutdown() will redeliver current message

        @task_postrun.connect  # Task finished
        def _consider_a_shutdown(*args, **kwargs):
            worker = self._worker
            worker.__task_finished = True
            logger.info('Job done. Is there time for one more? [task_postrun]')

            if self.is_time_up():
                logger.info('No time for another job. Now shutdown! [task_postrun]')
                self._demand_shutdown(*args, **kwargs)
            else:
                logger.info('There is still time for another job. Keep fetching! [task_postrun]')
                _set_job_watchdog()

        # Using weak references. Is up to the caller to clear the callbacks produced
        self.hooks = [_set_broker_watchdog, _set_job_watchdog, _unset_watchdogs, _ack_success]
        return self.hooks

    def _demand_shutdown(self, *args, **kwargs):
        worker = self._worker

        # Hack around "Worker shutdown creates duplicate messages in SQS broker"
        # (applies to any broker but 'amqp', if I understand correctly)
        # Celery issue #4002
        # See: https://github.com/celery/celery/issues/4002#issuecomment-377111157
        # See: https://gist.github.com/lovemyliwu/af5112de25b594205a76c3bfd00b9340
        worker.consumer.connection._default_channel.do_restore = False

        self._shutdown_worker()

# coding: utf-8
from __future__ import unicode_literals, absolute_import
import logging

from celery import Task, current_app as app

from celery_serverless import invoker

logger = logging.getLogger(__name__)

SERVERLESS_QUEUES = app.conf.get('serverless_queues', (app.conf.task_queues or [app.conf.task_default_queue]))


def trigger_invoke(task=None, *args, **kwargs):
    """Invokes the Serverless Function"""
    if 'queue' in kwargs and kwargs['queue'] not in SERVERLESS_QUEUES:
        logging.warning("Serverless worker will probable not get the task,"
                        " as its queue %s is probable not being listened there",
                        kwargs['queue'])
    return invoker.Invoker().invoke_main()


class TriggerServerlessMixin(object):
    """Do the standart Task enqueue, then invokes the Serverless Function"""
    def apply_async(self, *args, **kwargs):
        return trigger_invoke(self, *args, **kwargs)


class TriggerAfterQueueTask(Task, TriggerServerlessMixin):
    """Do the standart Task enqueue, then invokes the Serverless Function"""


class TriggerBeforeQueueTask(TriggerServerlessMixin, Task):
    """Invokes the Serverless Function, then do the standard Task enqueue"""
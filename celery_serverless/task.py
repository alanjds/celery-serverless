# coding: utf-8
from __future__ import unicode_literals, absolute_import

from celery import Task

from celery_serverless import invoker


def trigger_invoke(task, *args, **kwargs):
    """Invokes the Serverless Function"""
    return invoker.invoke_main()


class TriggerAfterQueueTask(Task):
    """Does the standart Task enqueue, then invokes the Serverless Function"""
    def apply_async(self, *args, **kwargs):
        result = super(__class__, self).apply_async(self, *args, **kwargs)
        trigger_invoke(self, *args, **kwargs)
        return result


class TriggerBeforeQueueTask(Task):
    """Invokes the Serverless Function, then does the standard Task enqueue"""
    def apply_async(self, *args, **kwargs):
        trigger_invoke(self, *args, **kwargs)
        return super(__class__, self).apply_async(self, *args, **kwargs)

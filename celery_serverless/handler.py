# coding: utf-8
import json
import logging

from .worker_management import spawn_worker, attach_hooks

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


def worker(event, context):
    try:
        remaining_seconds = context.get_remaining_time_in_millis() / 1000.0
    except Exception as e:
        logger.exception('Could not got remaining_seconds. Is the context right?')
        remaining_seconds = 5 * 60 # 5 minutes by default

    softlimit = remaining_seconds-30.0  # Poke the job 30sec before the abyss
    hardlimit = remaining_seconds-15.0  # Kill the job 15sec before the abyss

    hooks = attach_hooks()
    spawn_worker(
        softlimit=softlimit if softlimit > 5 else None,
        hardlimit=hardlimit if hardlimit > 5 else None,
    )  # Will block until one task got processed

    body = {
        "message": "Celery worker worked, lived, and died.",
    }

    return {"statusCode": 200, "body": json.dumps(body)}

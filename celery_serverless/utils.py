# coding: utf-8
import asyncio
from threading import Thread
from inspect import isawaitable

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


def aiorun_on_thread(coro):
    if not isawaitable(coro):
        raise TypeError('An awaitable should be provided')

    if not slave_thread:
        _start_thread()
    return asyncio.run_coroutine_threadsafe(coro, slave_loop)

import threading
import random
import asyncio

class CountdownTimer():
    def __init__(self, intervalMin, intervalMax, func):
        self._intervalMin = intervalMin
        self._intravalMax = intervalMax
        self._interval = random.uniform(self._intravalMax, self._intravalMax)
        self._func = func
        self._thread = threading.Timer(self._interval, self.handler)
        print("{:.2f}".format(self._interval))

    def start(self):
        self._thread.start()

    def reset(self):
        self._thread.cancel()
        self._interval = random.uniform(self._intravalMax, self._intravalMax)
        self._thread = threading.Timer(self._interval, self.handler)
        self._thread.start()

    def handler(self):
        asyncio.run(self._func())
        # self._func()
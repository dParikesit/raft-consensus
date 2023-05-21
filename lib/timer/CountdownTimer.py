import threading
import random
import asyncio

class CountdownTimer():
    def __init__(self, intervalMin, intervalMax, func):
        self._intervalMin = intervalMin
        self._intravalMax = intervalMax
        self._func = func
        self._thread = threading.Timer(random.uniform(self._intravalMax, self._intravalMax), self.handler)

    def start(self):
        self._thread.start()

    def reset(self):
        self._thread.cancel()
        self._thread = threading.Timer(random.uniform(self._intravalMax, self._intravalMax), self.handler)
        self._thread.start()

    def handler(self):
        asyncio.run(self._func())
        # self._func()
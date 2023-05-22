import threading
import random
import asyncio
from typing import Optional

class CountdownTimer():
    def __init__(self, func, intervalMin=None, intervalMax=None, interval = None):
        self._intervalMin: Optional[float] = intervalMin
        self._intervalMax: Optional[float] = intervalMax
        
        if interval:
            self._interval = interval
        elif self._intervalMin and self._intervalMax:
            self._interval = random.uniform(self._intervalMin, self._intervalMax)
        
        self._func = func
        self._thread = threading.Timer(self._interval, self.handler)
        print("{:.2f}".format(self._interval))

    def start(self):
        self._thread.start()
    
    def cancel(self):
        self._thread.cancel()

    def reset(self, interval: Optional[float]=None):
        self._thread.cancel()
        if interval:
            self._interval = interval
        elif self._intervalMin and self._intervalMax:
            self._interval = random.uniform(self._intervalMin, self._intervalMax)
        self._thread = threading.Timer(self._interval, self.handler)
        self._thread.start()

    def handler(self):
        asyncio.run(self._func())
        # self._func()
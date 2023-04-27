from typing import Deque
from collections import deque

class MessageQueue:
    __slots__ = ('_queue')

    def __init__(self) -> None:
        self._queue: Deque[str] = deque()
    
    def enqueue(self, item: str):
        self._queue.append(item)
    
    def dequeue(self) -> str:
        return self._queue.popleft()
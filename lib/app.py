from typing import Deque
from collections import deque

class MessageQueue:
    __slots__ = ('__queue')

    def __init__(self) -> None:
        self.__queue: Deque[str] = deque()
    
    def enqueue(self, item: str):
        self.__queue.append(item)
    
    def dequeue(self) -> str:
        return self.__queue.popleft()
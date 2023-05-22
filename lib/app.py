import re
from typing import Deque
from collections import deque

class MessageQueue:
    __slots__ = ('__queue')

    def __init__(self) -> None:
        self.__queue: Deque[str] = deque()

    def clear(self):
        self.__queue.clear()
    
    def enqueue(self, item: str):
        self.__queue.append(item)
    
    def dequeue(self) -> str:
        return self.__queue.popleft()
    
    def prepend(self, item: str):
        self.__queue.insert(0, item)
    
    def length(self) -> str:
        return str(len(self.__queue))
    
    def apply(self, operation: str) -> str:
        if operation=="dequeue":
            return self.dequeue()
        else:
            arr = re.split('\W+', operation)
            self.enqueue(arr[1])
            return self.length()

from typing import List, Tuple

from lib.struct.logEntry import LogEntry

class AppendEntriesBody:
    __slots__ = ('term', 'leaderId', 'prevLogIdx', 'prevLogTerm', 'entries', 'leaderCommit')

    def __init__(self, term: int, leaderId: int, prevLogIdx: int, prevLogTerm: int, entries: List[LogEntry], leaderCommit: int) -> None:
        self.term: int = term
        self.leaderId: int = leaderId
        self.prevLogIdx: int = prevLogIdx
        self.prevLogTerm: int = prevLogTerm
        self.entries: List[LogEntry] = entries
        self.leaderCommit: int = leaderCommit
    
class RequestVoteBody:
    __slots__ = ('term', 'candidateId', 'lastLogIdx', 'lastLogTerm')

    def __init__(self, term: int, candidateId: int, lastLogIdx: int, lastLogTerm: int) -> None:
        self.term: int = term
        self.candidateId: int = candidateId
        self.lastLogIdx: int = lastLogIdx
        self.lastLogTerm: int = lastLogTerm

class ClientRequestBody:
    __slots__ = ('requestNumber', 'command')

    def __init__(self, requestNumber: int, command: str) -> None:
        self.requestNumber: int = requestNumber
        self.command: int = command
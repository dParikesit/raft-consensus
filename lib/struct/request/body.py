from typing import List, Tuple

class AppendEntriesBody:
    __slots__ = ('term', 'leaderId', 'prevLogIndex', 'prevLogTerm', 'entries', 'leaderCommit')

    def __init__(self, term: int, leaderId: int, prevLogIndex: int, prevLogTerm: int, entries: List[Tuple[str, str]], leaderCommit: int) -> None:
        self.term = term
        self.leaderId = leaderId
        self.prevLogIndex = prevLogIndex
        self.prevLogTerm = prevLogTerm
        self.entries = entries
        self.leaderCommit = leaderCommit
    
class RequestVoteBody:
    __slots__ = ('term', 'candidateId', 'lastLogIndex', 'lastLogTerm')

    def __init__(self, term: int, candidateId: int, lastLogIndex: int, lastLogTerm: int) -> None:
        pass
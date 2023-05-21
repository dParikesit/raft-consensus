from typing import List, Tuple

from lib.struct.logEntry import LogEntry
from lib.struct.address import Address

class AppendEntriesBody():
    __slots__ = ('term', 'leaderId', 'prevLogIdx', 'prevLogTerm', 'entries', 'leaderCommit')

    def __init__(self, term: int, leaderId: Address, prevLogIdx: int, prevLogTerm: int, entries: List[LogEntry], leaderCommit: int) -> None:
        self.term: int = term
        self.leaderId: Address = leaderId
        self.prevLogIdx: int = prevLogIdx
        self.prevLogTerm: int = prevLogTerm
        self.entries: List[LogEntry] = entries
        self.leaderCommit: int = leaderCommit

    def __str__(self):
        return f"AppendEntriesBody(term={self.term}, leaderId={self.leaderId}, prevLogIdx={self.prevLogIdx}, prevLogTerm={self.prevLogTerm}, entries={self.entries}, leaderCommit={self.leaderCommit})"

class AppendEntriesMembershipBody:
    __slots__ = ('term', 'leaderId', 'prevLogIdx', 'prevLogTerm', 'entries', 'leaderCommit', 'cluster_addr_list')

    def __init__(self, term: int, leaderId: int, prevLogIdx: int, prevLogTerm: int, entries: List[LogEntry], leaderCommit: int, cluster_addr_list: list) -> None:
        self.term: int = term
        self.leaderId: int = leaderId
        self.prevLogIdx: int = prevLogIdx
        self.prevLogTerm: int = prevLogTerm
        self.entries: List[LogEntry] = entries
        self.leaderCommit: int = leaderCommit
        self.cluster_addr_list: list = cluster_addr_list

    def __str__(self):
        return f"AppendEntriesMembershipBody(term={self.term}, leaderId={self.leaderId}, prevLogIdx={self.prevLogIdx}, prevLogTerm={self.prevLogTerm}, entries={self.entries}, leaderCommit={self.leaderCommit}, cluster_addr_list={self.cluster_addr_list})"

class RequestVoteBody:
    __slots__ = ('term', 'candidateId', 'lastLogIdx', 'lastLogTerm')

    def __init__(self, term: int, candidateId: Address, lastLogIdx: int, lastLogTerm: int) -> None:
        self.term: int = term
        self.candidateId: Address = candidateId
        self.lastLogIdx: int = lastLogIdx
        self.lastLogTerm: int = lastLogTerm

class ClientRequestBody:
    __slots__ = ('clientID', 'requestNumber', 'command')

    def __init__(self, clientID: str, requestNumber: int, command: str) -> None:
        self.clientID: str = clientID
        self.requestNumber: int = requestNumber
        self.command: str = command
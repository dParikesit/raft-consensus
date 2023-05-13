from json import JSONDecoder, JSONEncoder, dumps
from typing import Any, List, Tuple

from lib.struct.address import Address
from lib.struct.logEntry import LogEntry


class ResponseEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, AppendEntriesResponse):
            return {
                "type": o.type,
                "term": o.term,
                "success": o.success
            }
        if isinstance(o, RequestVoteResponse):
            return {
                "type": o.type,
                "term": o.term,
                "voteGranted": o.voteGranted
            }
        if isinstance(o, MembershipResponse):
            return {
                "type": o.type,
                "status": o.status,
                "address": o.address,
                "log": o.log,
                "cluster_addr_list": o.cluster_addr_list
            }
        
        if isinstance(o, Response):
            return {"type": o.type}
        return super().default(o)
    
class ResponseDecoder(JSONDecoder):
    def __init__(self, *args, **kwargs):
        JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)
    
    def object_hook(self, obj):
        if isinstance(obj, dict) and "type" in obj:
            if obj["type"] == 'AppendEntriesResponse':
                return AppendEntriesResponse(obj["term"], obj["success"])
            if obj["type"] == 'RequestVoteResponse':
                return RequestVoteResponse(obj["term"], obj["voteGranted"])
            if obj["type"] == 'MembershipResponse':
                return MembershipResponse(obj["status"], Address(obj["address"]["ip"], obj["address"]["port"]), obj["log"], obj["cluster_addr_list"])
        return obj

class Response:
    __slots__ = ('type')

    def __init__(self, type: str) -> None:
        self.type: str = type

    def __str__(self) -> str:
        return dumps(self, indent=2, cls=ResponseEncoder)

    def __repr__(self) -> str:
        return self.__str__()

class MembershipResponse(Response):
    __slots__ = ('status', 'address', 'log', 'cluster_addr_list')

    def __init__(self, status: str, address: Address, log: List[LogEntry] = [], cluster_addr_list: List[Address] = []) -> None:
        super().__init__('MembershipResponse')
        self.status: str                        = status
        self.address: Address                   = address
        self.log: List[LogEntry]                = log
        self.cluster_addr_list:   List[Address] = cluster_addr_list

class AppendEntriesResponse(Response):
    __slots__ = ('term', 'success')

    def __init__(self, term: int, success: bool) -> None:
        super().__init__("AppendEntriesResponse")
        self.term:      int     = term
        self.success:   bool    = success

class RequestVoteResponse(Response):
    __slots__ = ('term', 'voteGranted')

    def __init__(self, term: int, voteGranted: bool) -> None:
        super().__init__("RequestVoteResponse")
        self.term:          int     = term
        self.voteGranted:   bool    = voteGranted
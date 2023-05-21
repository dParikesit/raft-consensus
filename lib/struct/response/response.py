from json import JSONDecoder, JSONEncoder, dumps
from typing import Any, List, Tuple, Optional

from lib.struct.address import Address
from lib.struct.logEntry import LogEntry


class ResponseEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, Address):
            return {
                "ip": o.ip,
                "port": o.port
            }
        if isinstance(o, LogEntry):
            return{
                "term": o.term,
                "idx": o.idx,
                "clientId": o.clientId,
                "operation": o.operation,
                "reqNum": o.reqNum,
                "result": o.result
            }
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
        if isinstance(o, ClientRequestResponse):
            return {
                "type": o.type,
                "requestNumber": o.requestNumber,
                "status": o.status,
                "result": o.result
            }
        if isinstance(o, ClientRiderectResponse):
            return{
                "type"  : o.type,
                "status": o.status,
                "address"  : o.address
            }
        if isinstance(o, ClientRequestLogResponse):
            return{
                "type"          : o.type,
                "status"        : o.status,
                "requestNumber" : o.requestNumber,
                "log"           : o.log
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
                return MembershipResponse(obj["status"], Address(obj["address"]["ip"], obj["address"]["port"]), [LogEntry(elem["term"], elem["idx"], elem["clientId"], elem["operation"], elem["reqNum"], elem["result"]) for elem in obj["log"]], obj["cluster_addr_list"])
            if obj["type"] == 'ClientRequestResponse':
                return ClientRequestResponse(obj["requestNumber"], obj["status"], obj["result"])
            if obj["type"] == 'ClientRiderectResponse':
                return ClientRiderectResponse(obj["status"], obj["address"])
            if obj["type"] == 'ClientRequestLogResponse':
                return ClientRequestLogResponse(obj["status"], obj["requestNumber"], obj["log"])
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

class ClientRequestResponse(Response):
    __slots__ = ('requestNumber', 'status', 'result')

    def __init__(self, requestNumber: int, status: str, result: Optional[str]) -> None:
        super().__init__("ClientRequestResponse")
        self.requestNumber: int     = requestNumber
        self.status:   str          = status
        self.result:        str     = result

class ClientRiderectResponse(Response):
    __slots__ = ('status', 'address')

    def __init__(self, status: str, address: Address) -> None:
        super().__init__("ClientRiderectResponse")
        self.status:    str         = status
        self.address:   Address     = address

class ClientRequestLogResponse(Response):
    __slots__ = ('status', 'requestNumber', 'log')

    def __init__(self, status: str, requestNumber: int, log: List[LogEntry]) -> None:
        super().__init__("ClientRequestLogResponse")
        self.status:        str             = status
        self.requestNumber: int             = requestNumber
        self.log:           List[LogEntry]  = log
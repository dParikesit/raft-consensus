from json import JSONDecoder, JSONEncoder, dumps
from typing import Any, List, Optional, Tuple

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
                "isOp": o.isOp,
                "clientId": o.clientId,
                "operation": o.operation,
                "reqNum": o.reqNum,
                "result": o.result
            }
        if isinstance(o, AppendEntriesResponse):
            return {
                "type": o.type,
                "dest": o.dest,
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
                "leader": o.leader,
                "cluster_addr_list": o.cluster_addr_list
            }
        if isinstance(o, ClientRequestResponse):
            return {
                "type": o.type,
                "requestNumber": o.requestNumber,
                "status": o.status,
                "result": o.result
            }
        if isinstance(o, ClientRedirectResponse):
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
        if isinstance(o, ConfigChangeResponse):
            return{
                "type": o.type,
                "dest": o.dest,
                "success": o.success
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
                return AppendEntriesResponse(Address(obj["dest"]["ip"], obj["dest"]["port"]), obj["term"], obj["success"])
            if obj["type"] == 'RequestVoteResponse':
                return RequestVoteResponse(obj["term"], obj["voteGranted"])
            if obj["type"] == 'MembershipResponse':
                return MembershipResponse(obj["status"], Address(obj["leader"]["ip"], obj["leader"]["port"]), [Address(elem["ip"], elem["port"]) for elem in obj["cluster_addr_list"]])
            if obj["type"] == 'ClientRequestResponse':
                return ClientRequestResponse(obj["requestNumber"], obj["status"], obj["result"])
            if obj["type"] == 'ClientRedirectResponse':
                return ClientRedirectResponse(obj["status"], obj["address"])
            if obj["type"] == 'ClientRequestLogResponse':
                return ClientRequestLogResponse(obj["status"], obj["requestNumber"], [LogEntry(elem["term"], elem["isOp"], elem["clientId"], elem["operation"], elem["reqNum"], elem["result"]) for elem in obj["log"]])
            if obj["type"] == "ConfigChangeResponse":
                return ConfigChangeResponse(Address(obj["dest"]["ip"], obj["dest"]["port"]), obj["success"])
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
    __slots__ = ('status', 'leader', 'log', 'cluster_addr_list')

    def __init__(self, status: str, leader: Address, cluster: List[Address] = []) -> None:
        super().__init__('MembershipResponse')
        self.status: str                        = status
        self.leader: Address                    = leader
        self.cluster_addr_list: List[Address]   = cluster

class AppendEntriesResponse(Response):
    __slots__ = ('dest','term', 'success')

    def __init__(self, dest: Address, term: int, success: bool) -> None:
        super().__init__("AppendEntriesResponse")
        self.dest: Address = dest
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
        self.requestNumber: int             = requestNumber
        self.status:        str             = status
        self.result:        Optional[str]   = result

class ClientRedirectResponse(Response):
    __slots__ = ('status', 'address')

    def __init__(self, status: str, address: Optional[Address]) -> None:
        super().__init__("ClientRedirectResponse")
        self.status:    str                 = status
        self.address:   Optional[Address]   = address

class ClientRequestLogResponse(Response):
    __slots__ = ('status', 'requestNumber', 'log')

    def __init__(self, status: str, requestNumber: int, log: List[LogEntry]) -> None:
        super().__init__("ClientRequestLogResponse")
        self.status:        str             = status
        self.requestNumber: int             = requestNumber
        self.log:           List[LogEntry]  = log

class ConfigChangeResponse(Response):
    __slots__ = ('dest','success')

    def __init__(self, dest: Address, success: bool) -> None:
        super().__init__("ConfigChangeResponse")
        self.dest: Address = dest
        self.success: bool = success
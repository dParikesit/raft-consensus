from json import JSONDecoder, JSONEncoder, dumps
from typing import Any, List, Tuple

from lib.struct.address import Address
from lib.struct.logEntry import LogEntry


class ResponseEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, MembershipResponse):
            return {
                "type": o.type,
                "status": o.status,
                "address": {
                    "ip": o.address.ip,
                    "port": o.address.port
                },
                "log": o.log,
                "cluster_addr_list": o.cluster_addr_list
            }
        if isinstance(o, Response):
            return {"type": o.type, "status": o.status}
        return super().default(o)
    
class ResponseDecoder(JSONDecoder):
    def __init__(self, *args, **kwargs):
        JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)
    
    def object_hook(self, obj):
        if isinstance(obj, dict) and "type" in obj:
            if obj["type"] == 'MembershipResponse':
                return MembershipResponse(obj["status"], Address(obj["address"]["ip"], obj["address"]["port"]), obj["log"], obj["cluster_addr_list"])
        return obj

class Response:
    __slots__ = ('status', 'type')

    def __init__(self, status: str, type: str) -> None:
        self.status: str = status
        self.type: str = type

    def __str__(self) -> str:
        return dumps(self, indent=2, cls=ResponseEncoder)

    def __repr__(self) -> str:
        return self.__str__()

class MembershipResponse(Response):
    __slots__ = ('address', 'log', 'cluster_addr_list')

    def __init__(self, status: str, address: Address, log: List[LogEntry] = [], cluster_addr_list: List[Address] = []) -> None:
        super().__init__(status, 'MembershipResponse')
        self.address: Address                   = address
        self.log: List[LogEntry]                = log
        self.cluster_addr_list:   List[Address] = cluster_addr_list
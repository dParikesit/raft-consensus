from json import JSONEncoder, JSONDecoder, dumps
from typing import Any, List, Tuple

from lib.struct.address import Address


class ResponseEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, MembershipResponse):
            return {
                "status": o.status,
                "address": {
                    "ip": o.address.ip,
                    "port": o.address.port
                },
                "log": o.log,
                "cluster_addr_list": o.cluster_addr_list
            }
        if isinstance(o, Response):
            return {"status": o.status}
        return super().default(o)
    
class ResponseDecoder(JSONDecoder):
    def __init__(self, *args, **kwargs):
        JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)
    
    def object_hook(self, obj):
        if isinstance(obj, dict):
            if "status" in obj and "address" in obj and "log" in obj and "cluster_addr_list" in obj:
                return MembershipResponse(obj["status"], Address(obj["address"]["ip"], obj["address"]["port"]), obj["log"], obj["cluster_addr_list"])
        return obj

class Response:
    __slots__ = ('status')

    def __init__(self, status: str) -> None:
        self.status: str = status

    def __str__(self) -> str:
        return dumps(self, cls=ResponseEncoder)

    def __repr__(self) -> str:
        return self.__str__()

class MembershipResponse(Response):
    __slots__ = ('address', 'log', 'cluster_addr_list')

    def __init__(self, status: str, address: Address, log: List[Tuple[str, str]] = [], cluster_addr_list: List[Address] = []) -> None:
        super().__init__(status)
        self.address: Address                   = address
        self.log: List[Tuple[str, str]]         = log
        self.cluster_addr_list:   List[Address] = cluster_addr_list
from json import JSONDecoder, JSONEncoder, dumps
from typing import Any, List, Tuple

from lib.struct.address import Address


class RequestEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, StringRequest):
            return {
                "type": o.type, 
                "dest": o.dest, 
                "func_name": o.func_name,
                "body": o.body
            }
        if isinstance(o, AddressRequest):
            return {
                "type": o.type, 
                "dest": o.dest, 
                "func_name": o.func_name,
                "body": {
                    "ip": o.body.ip,
                    "port": o.body.port
                }
            }
        if isinstance(o, Request):
            return {"type": o.type, "dest": o.dest, "func_name": o.func_name}
        return super().default(o)
    
class RequestDecoder(JSONDecoder):
    def __init__(self, *args, **kwargs):
        JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)
    
    def object_hook(self, obj):
        if isinstance(obj, dict) and "type" in obj:
            if obj["type"] == 'StringRequest':
                return StringRequest(obj["dest"], obj["func_name"], obj["body"])
            if obj["type"] == 'AddressRequest':
                return AddressRequest(obj["dest"], obj["func_name"], Address(obj["body"]["ip"], obj["body"]["port"]))
        return obj

class Request:
    __slots__ = ('type','dest', 'func_name', 'body')

    def __init__(self, type: str, dest: Address, func_name: str) -> None:
        self.type           = type
        self.dest: Address  = dest
        self.func_name: str = func_name

    def __str__(self) -> str:
        return dumps(self, cls=RequestEncoder)

    def __repr__(self) -> str:
        return self.__str__()
    
class StringRequest(Request):
    def __init__(self, dest: Address, func_name: str, body: str) -> None:
        super().__init__("StringRequest", dest, func_name)
        self.body = body

class AddressRequest(Request):
    def __init__(self, dest: Address, func_name: str, body: Address) -> None:
        super().__init__("AddressRequest", dest, func_name)
        self.body = body
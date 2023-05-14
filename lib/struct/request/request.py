from json import JSONDecoder, JSONEncoder, dumps
from typing import Any, List, Tuple

from lib.struct.address import Address
from lib.struct.logEntry import LogEntry
from lib.struct.request.body import AppendEntriesBody, RequestVoteBody, ClientRequestBody


class RequestEncoder(JSONEncoder):
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
        if isinstance(o, AppendEntriesBody):
            return{
                "term": o.term,
                "leaderId": o.leaderId,
                "prevLogIdx": o.prevLogIdx,
                "prevLogTerm": o.prevLogTerm,
                "entries": o.entries,
                "leaderCommit": o.leaderCommit
            }
        if isinstance(o, RequestVoteBody):
            return {
                "term": o.term,
                "candidateId": o.candidateId,
                "lastLogIdx": o.lastLogIdx,
                "lastLogTerm": o.lastLogTerm
            }
        if isinstance(o, ClientRequestBody):
            return{
                "requestNumber": o.requestNumber,
                "command": o.command
            }
        if isinstance(o, Request):
            return {
                "type": o.type, 
                "dest": o.dest, 
                "func_name": o.func_name, 
                "body": o.body
            }
        return super().default(o)
    
class RequestDecoder(JSONDecoder):
    def __init__(self, *args, **kwargs):
        JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)
    
    def object_hook(self, obj):
        if isinstance(obj, dict) and "type" in obj:
            if obj["type"] == 'AppendEntriesRequest':
                return AppendEntriesRequest(obj["dest"], obj["func_name"], AppendEntriesBody(obj["body"]["term"], obj["body"]["leaderId"], obj["body"]["prevLogIdx"], obj["body"]["prevLogTerm"], obj["body"]["entries"], obj["body"]["leaderCommit"]))
            if obj["type"] == 'RequestVoteEntries':
                return RequestVoteRequest(obj["dest"], obj["func_name"], RequestVoteBody(obj["body"]["term"], obj["body"]["candidateId"], obj["body"]["lastLogIdx"], obj["body"]["lastLogTerm"]))
            if obj["type"] == 'StringRequest':
                return StringRequest(obj["dest"], obj["func_name"], obj["body"])
            if obj["type"] == 'AddressRequest':
                return AddressRequest(obj["dest"], obj["func_name"], Address(obj["body"]["ip"], obj["body"]["port"]))
            if obj["type"] == 'ClientRequest':
                return ClientRequest(obj["dest"], obj["func_name"], ClientRequestBody(obj["body"]["requestNumber"], obj["body"]["command"]))
        return obj

class Request:
    __slots__ = ('type', 'dest', 'func_name', 'body')

    def __init__(self, type: str, dest: Address, func_name: str) -> None:
        self.type: str      = type
        self.dest: Address  = dest
        self.func_name: str = func_name

    def __str__(self) -> str:
        return dumps(self, cls=RequestEncoder)

    def __repr__(self) -> str:
        return self.__str__()
    
class StringRequest(Request):
    def __init__(self, dest: Address, func_name: str, body: str) -> None:
        super().__init__("StringRequest", dest, func_name)
        self.body: str = body

class AddressRequest(Request):
    def __init__(self, dest: Address, func_name: str, body: Address) -> None:
        super().__init__("AddressRequest", dest, func_name)
        self.body: Address = body

class AppendEntriesRequest(Request):
    def __init__(self, dest: Address, func_name: str, body: AppendEntriesBody) -> None:
        super().__init__("AppendEntriesRequest", dest, func_name)
        self.body: AppendEntriesBody = body

class RequestVoteRequest(Request):
    def __init__(self, dest: Address, func_name: str, body: RequestVoteBody) -> None:
        super().__init__("RequestVoteRequest", dest, func_name)
        self.body: RequestVoteBody = body

class ClientRequest(Request):
    def __init__(self, dest: Address, func_name: str, body: ClientRequestBody) -> None:
        super().__init__("ClientRequest", dest, func_name)
        self.body: ClientRequestBody = body
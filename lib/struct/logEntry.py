from typing import Optional, Any
from json import dumps, JSONEncoder


class LogEntryEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, LogEntry):
            return{
                "term": o.term,
                "clientId": o.clientId,
                "operation": o.operation,
                "reqNum": o.reqNum,
                "result": o.result
            }
        return super().default(o)

class LogEntry():
    # Ini boleh diganti. Aku belom kepikiran log nya mau diisi apa aja
    __slots__ = ('term', 'isOp', 'clientId', 'operation', 'reqNum', 'result')

    # term isinya term sekarang
    # isOp boolean, if true then client operation, else add server
    # clientId isinya id client. berguna buat nanti exactly once
    # operation ya isinya operation
    # reqNum isinya reqNum. Buat exactly once juga
    # result isinya hasil operasi

    def __init__(self, term: int, isOp: bool, clientId: str, operation: str, reqNum: int, result:  Optional[str]) -> None:
        self.term: int              = term
        self.isOp: bool             = isOp
        self.clientId: str          = clientId
        self.operation: str         = operation
        self.reqNum: int            = reqNum
        self.result: Optional[str]  = result
    
    def __str__(self) -> str:
        return dumps(self, cls=LogEntryEncoder)

    def __repr__(self) -> str:
        return self.__str__()
from typing import Optional
from lib.struct.address import Address
class LogEntry:
    # Ini boleh diganti. Aku belom kepikiran log nya mau diisi apa aja
    __slots__ = ('term', 'clientId', 'operation', 'reqNum', 'result')

    # term isinya term sekarang
    # clientId isinya address client. berguna buat nanti exactly once
    # operation ya isinya operation
    # reqNum isinya reqNum. Buat exactly once juga
    # result isinya hasil operasi

    def __init__(self, term: int, clientId: Address, operation: str, reqNum: int, result:  Optional[str]) -> None:
        self.term: int              = term
        self.clientId: Address      = clientId
        self.operation: str         = operation
        self.reqNum: int            = reqNum
        self.result: Optional[str]  = result
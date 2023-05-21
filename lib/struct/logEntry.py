from typing import Optional
class LogEntry():
    # Ini boleh diganti. Aku belom kepikiran log nya mau diisi apa aja
    __slots__ = ('term', 'idx', 'clientId', 'operation', 'reqNum', 'result')

    # term isinya term sekarang
    # clientId isinya id client. berguna buat nanti exactly once
    # operation ya isinya operation
    # reqNum isinya reqNum. Buat exactly once juga
    # result isinya hasil operasi

    def __init__(self, term: int, idx: int, clientId: str, operation: str, reqNum: int, result:  Optional[str]) -> None:
        self.term: int              = term
        self.idx: int               = idx
        self.clientId: str          = clientId
        self.operation: str         = operation
        self.reqNum: int            = reqNum
        self.result: Optional[str]  = result
class LogEntry:
    # Ini boleh diganti. Aku belom kepikiran log nya mau diisi apa aja
    __slots__ = ('timestamp', 'term', 'operation', 'status')

    def __init__(self, term: int, operation: str, status:  str) -> None:
        self.term: int      = term
        self.operation: str = operation
        self.status: str    = status
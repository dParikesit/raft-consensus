# python3 client.py localhost 3000 enqueue(5)
# python3 client.py localhost 3000 dequeue
# dimana localhost 3000 nya tuh server

import json
import sys
import time
from enum import Enum
from typing import Any, Optional
from xmlrpc.client import ServerProxy

from lib.struct.request.request import Request, RequestEncoder
from lib.struct.response.response import Response, ResponseDecoder


class ExecuteCmd(Enum):
    ENQUEUE = 1
    DEQUEUE = 2

class Client:
    __slots__ = ("ip", "port", "server")

    def __init__(self, ip: str, port: int, server: ServerProxy) -> None:
        self.ip = ip
        self.port = port
        self.server = server
    
    def __print_response(self, res: Response):
        if isinstance(res, Response):
            print(f"[{self.ip}:{self.port}] [{time.strftime('%H:%M:%S')}] {res.status}")
        # Disini kamu bikin class Response di response.py sebagai template response buat method execute n request_log

    def __send_request(self, req: Request) -> None:
        json_request = json.dumps(req, cls=RequestEncoder)
        rpc_function = getattr(self.server, req.func_name)
        response = json.loads(rpc_function(json_request), cls=ResponseDecoder)
        self.__print_response(response)

    def execute(self, command: ExecuteCmd, param: Optional[str]):
        # Command yang boleh cuma enqueue(angka) dan dequeue
        if command == ExecuteCmd.ENQUEUE:
            if param is not None:
                print("enqueue")
                # self.__send_request()
        elif command == ExecuteCmd.DEQUEUE:
            print("dequeue")
            pass
            # self.__send_request()
        else:
            print("else")
            raise Exception("Execute command error")
        
    def request_log(self):
        pass
        # self.__send_request()
        

if __name__ == "__main__":
    print("Starting client")
    
    server = ServerProxy(f"http://{sys.argv[1]}:{int(sys.argv[2])}")

    # Trus disini kamu coba bikin handling what if server nya bukan leader
    # Kalo mau bikin method check leader di raft.py boleh aja
    
    # Nah kalo handling nya dah selesai
    client = Client(sys.argv[1], int(sys.argv[2]), server)

    # Nah kalo udah baru input command dkk. Di wrap pake while true dkk serah kamu
    try:
        command = 0
        param = None
        if (sys.argv[3] == "enqueue") :
            command = ExecuteCmd(1)
            param = sys.argv[4]
        elif (sys.argv[3] == "dequeue"):
            command = ExecuteCmd(2)
        
        client.execute(command, param)
        
    except Exception as e: 
        print(e)
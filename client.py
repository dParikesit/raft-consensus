# python3 client.py localhost 3000 enqueue(5)
# python3 client.py localhost 3000 dequeue
# dimana localhost 3000 nya tuh server

import json
import sys
import time
from enum import Enum
from typing import Any, Optional
from xmlrpc.client import ServerProxy

from lib.struct.request.request import Request, RequestEncoder, ClientRequest
from lib.struct.response.response import Response, ResponseDecoder, ClientRequestResponse
from lib.struct.request.body import ClientRequestBody


class ExecuteCmd(Enum):
    ENQUEUE = 1
    DEQUEUE = 2

class Client:
    __slots__ = ("ip", "port", "server")

    def __init__(self, ip: str, port: int, server: ServerProxy) -> None:
        self.ip = ip
        self.port = port
        self.server = server
    
    def __print_response(self, res: ClientRequestResponse):
        if isinstance(res, Response):
            print(f"[{self.ip}:{self.port}] [{time.strftime('%H:%M:%S')}] Request ({res.requestNumber}) {res.status}!")
        # Disini kamu bikin class Response di response.py sebagai template response buat method execute n request_log

    def __send_request(self, req: ClientRequest) -> Any:
        json_request = json.dumps(req, cls=RequestEncoder)
        rpc_function = getattr(self.server, req.func_name)
        response = json.loads(rpc_function(json_request), cls=ResponseDecoder)

        return response
    
    def __print_request(self, res: ClientRequest):
        print(f"[{self.ip}:{self.port}] [{time.strftime('%H:%M:%S')}] Request ({res.body.requestNumber}) {res.body.command} sent!")


    def execute(self, command: ExecuteCmd, param: Optional[str]):
        # Command yang boleh cuma enqueue(angka) dan dequeue
        if command == ExecuteCmd.ENQUEUE:
            if param is not None:
                contact_addr = self.ip + ":" + str(self.port)
                requestBody = ClientRequestBody(1, f"execute({param})")
                request = ClientRequest(contact_addr, "execute", requestBody)
                response = ClientRequestResponse(1, "failed")

                while response.status != "success":
                    self.__print_request(request)
                    response = self.__send_request(request)

                self.__print_response(response)
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
    print("Starting client\n")
    contact_addr = sys.argv[1] + ":" + sys.argv[2]
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
# python3 client.py localhost 3000
# contoh command: enqueue(5)
# buat end, command: -1
# dimana localhost 3000 nya tuh server

import json
import sys
import time
import re
from enum import Enum
from typing import Any, Optional
from xmlrpc.client import ServerProxy

from lib.struct.request.request import Request, RequestEncoder, ClientRequest
from lib.struct.response.response import Response, ResponseDecoder, ClientRequestResponse
from lib.struct.request.body import ClientRequestBody
from lib.struct.address import Address


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
                contact_addr = Address(self.ip, int(self.port))
                requestBody = ClientRequestBody(1, param)
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
    print("Starting client")
    server = ServerProxy(f"http://{sys.argv[1]}:{int(sys.argv[2])}")

    # Trus disini kamu coba bikin handling what if server nya bukan leader
    # Kalo mau bikin method check leader di raft.py boleh aja
    
    # Nah kalo handling nya dah selesai
    client = Client(sys.argv[1], int(sys.argv[2]), server)

    value = ""
    patternEnq = r"enqueue\(\d+\)"
    patternDeq = r"dequeue"

    while (value != "-1"):
        value= input("\nCommand ('-1' to end connection): ")

        try:
            command = ExecuteCmd.ENQUEUE
            param = None
            if (re.match(patternEnq, value)) :
                command = ExecuteCmd.ENQUEUE
                param = value
            elif (re.match(patternDeq, value)):
                command = ExecuteCmd.DEQUEUE
            elif (value != "-1"):
                print(f"[{client.ip}:{client.port}] [{time.strftime('%H:%M:%S')}] Wrong command!")
            
            client.execute(command, param)
            
        except Exception as e: 
            print(e)

    print(f"[{client.ip}:{client.port}] [{time.strftime('%H:%M:%S')}] Connection ended!\n")
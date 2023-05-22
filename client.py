# python3 client.py localhost 3000 client01
# contoh 
# command: enqueue(5)
# command: dequeue
# command: log
# buat end, command: -1

import json
import re
import sys
import time
import xmlrpc.client
from enum import Enum
from typing import Any, Optional
from xmlrpc.client import ServerProxy

from lib.struct.address import Address
from lib.struct.logEntry import LogEntry
from lib.struct.request.body import ClientRequestBody
from lib.struct.request.request import ClientRequest, Request, RequestEncoder
from lib.struct.response.response import (
    ClientRedirectResponse,
    ClientRequestLogResponse,
    ClientRequestResponse,
    Response,
    ResponseDecoder,
)


class TimeoutTransport(xmlrpc.client.Transport):
    def __init__(self, timeout=None, *args, **kwargs):
        self.timeout = timeout
        super().__init__(*args, **kwargs)

    def make_connection(self, host):
        connection = super().make_connection(host)
        if self.timeout is not None:
            connection.timeout = self.timeout
        return connection


class Client:
    __slots__ = ("ip", "port", "server", "clientID")

    def __init__(self, ip: str, port: int, server: ServerProxy, clientID: str) -> None:
        self.ip = ip
        self.port = port
        self.server = server
        self.clientID = clientID

    def __print_response(self, res: Response):
        # Response success or failed
        if isinstance(res, ClientRequestResponse):
            print(f"[{self.ip}:{self.port}] [{time.strftime('%H:%M:%S')}] [{self.clientID}] Request ({res.requestNumber}) {res.status}!")

        # Response redirect
        if isinstance(res, ClientRedirectResponse):
            if res.status == "Redirect":
                print(f"[{self.ip}:{self.port}] [{time.strftime('%H:%M:%S')}] [{self.clientID}] Request redirected to [{self.ip}:{self.port}]!\n")
            else:
                print(f"[{self.ip}:{self.port}] [{time.strftime('%H:%M:%S')}] [{self.clientID}] Request failed")

        if isinstance(res, ClientRequestLogResponse):
            print(f"[{self.ip}:{self.port}] [{time.strftime('%H:%M:%S')}] [{self.clientID}] Request Log_Leader {res.status}!")
            if len(res.log) == 0:
                print(f"[{self.ip}:{self.port}] [{time.strftime('%H:%M:%S')}] [{self.clientID}] Log is Empty!\n")
            else:
                for i in range(len(res.log)):
                    print(f"    [term: {res.log[i].term}] [idx: {res.log[i].idx}] [ClientID: {res.log[i].clientId}] Request ({res.log[i].reqNum}) {res.log[i].operation}")

    def __send_request(self, req: ClientRequest) -> Any:
        json_request = json.dumps(req, cls=RequestEncoder)
        rpc_function = getattr(self.server, req.func_name)
        response = json.loads(rpc_function(json_request), cls=ResponseDecoder)

        return response

    def __print_request(self, res: ClientRequest):
        if res.func_name == "execute":
            print(f"[{self.ip}:{self.port}] [{time.strftime('%H:%M:%S')}] [{self.clientID}] Request ({res.body.requestNumber}) {res.body.command} sent!")
        else:
            print(f"[{self.ip}:{self.port}] [{time.strftime('%H:%M:%S')}] [{self.clientID}] Request Leader_Log sent!")

    def execute(self, param: str, requestNumber: int):
        # Command yang boleh cuma enqueue(angka) dan dequeue
        if param is not None:
            contact_addr = Address(self.ip, int(self.port))
            requestBody = ClientRequestBody(self.clientID, requestNumber, param)
            request = ClientRequest(contact_addr, "execute", requestBody)
            response = ClientRequestResponse(requestNumber, "failed", None)

            no_leader = False

            while (
                response.status != "success" and requestNumber == response.requestNumber
            ):
                try:
                    self.__print_request(request)
                    response = self.__send_request(request)

                    # Client contacted follower, not leader
                    if response.status == "Redirect":
                        self.__print_response(response)
                        self.ip = response.address["ip"]
                        self.port = response.address["port"]
                        self.server = ServerProxy(
                            f"http://{self.ip}:{self.port}",
                            transport=TimeoutTransport(timeout=100),
                        )

                        contact_addr = Address(self.ip, int(self.port))
                        request = ClientRequest(contact_addr, "execute", requestBody)
                        response = ClientRequestResponse(requestNumber, "failed", None)
                    elif response.status == "No Leader":
                        no_leader = True
                        break

                except ConnectionRefusedError:
                    raise Exception(
                        "No connection could be made because the target machine actively refused it"
                    )

                except Exception as e:
                    print(
                        f"[{self.ip}:{self.port}] [{time.strftime('%H:%M:%S')}] [{self.clientID}] Timeout!\n"
                    )

            self.__print_response(response)
            if no_leader:
                raise Exception("No Leader in Connection")

    def request_log(self, requestNumber: int):
        contact_addr = Address(self.ip, int(self.port))
        requestBody = ClientRequestBody(self.clientID, requestNumber, "request_log")
        request = ClientRequest(contact_addr, "request_log", requestBody)
        response = ClientRequestResponse(requestNumber, "failed", None)

        no_leader = False

        while response.status != "success" and requestNumber == response.requestNumber:
            try:
                self.__print_request(request)
                response = self.__send_request(request)

                # Client contacted follower, not leader
                if response.status == "Redirect":
                    self.__print_response(response)
                    self.ip = response.address["ip"]
                    self.port = response.address["port"]
                    self.server = ServerProxy(
                        f"http://{self.ip}:{self.port}",
                        transport=TimeoutTransport(timeout=100),
                    )

                    contact_addr = Address(self.ip, int(self.port))
                    request = ClientRequest(contact_addr, "request_log", requestBody)
                    response = ClientRequestResponse(requestNumber, "failed", None)
                elif response.status == "No Leader":
                    no_leader = True
                    break

            except ConnectionRefusedError:
                raise Exception(
                    "No connection could be made because the target machine actively refused it"
                )

            except Exception as e:
                print(
                    f"[{self.ip}:{self.port}] [{time.strftime('%H:%M:%S')}] [{self.clientID}] Timeout!\n"
                )
        self.__print_response(response)
        if no_leader:
            raise Exception("No Leader in Connection")


if __name__ == "__main__":
    print("Starting client")
    server = ServerProxy(
        f"http://{sys.argv[1]}:{int(sys.argv[2])}",
        transport=TimeoutTransport(timeout=100),
    )

    # Nah kalo handling nya dah selesai
    client = Client(sys.argv[1], int(sys.argv[2]), server, sys.argv[3])

    value = ""
    patternEnq = r"enqueue\(\d+\)"

    requestNumber = 0

    while value != "-1":
        value = input("\nCommand ('-1' to end connection): ")

        try:
            param = ""
            if re.match(patternEnq, value) or value == "dequeue":
                param = value
                requestNumber += 1
                client.execute(param, requestNumber)
            elif value == "log":
                client.request_log(requestNumber)
            elif value != "-1":
                print(
                    f"[{client.ip}:{client.port}] [{time.strftime('%H:%M:%S')}] Wrong command!"
                )

        except Exception as e:
            print(f"[{client.ip}:{client.port}] [{time.strftime('%H:%M:%S')}] {e}!")
            break

    print(
        f"[{client.ip}:{client.port}] [{time.strftime('%H:%M:%S')}] Connection ended!\n"
    )

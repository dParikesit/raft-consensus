import asyncio
import json
import socket
import time
from enum import Enum
from threading import Thread
from typing import Any, List, Optional, Tuple
from xmlrpc.client import ServerProxy

from lib.struct.address import Address
from lib.struct.request.body import AppendEntriesBody
from lib.struct.response.response import ResponseEncoder, ResponseDecoder, Response, MembershipResponse, ClientRequestResponse, AppendEntriesResponse
from lib.struct.request.request import ClientRequest, Request, RequestEncoder, RequestDecoder, StringRequest, AddressRequest, AppendEntriesRequest
from lib.struct.logEntry import LogEntry


class RaftNode:
    HEARTBEAT_INTERVAL   = 1
    ELECTION_TIMEOUT_MIN = 2
    ELECTION_TIMEOUT_MAX = 3
    RPC_TIMEOUT          = 0.5

    class NodeType(Enum):
        LEADER    = 1
        CANDIDATE = 2
        FOLLOWER  = 3

    def __init__(self, application : Any, addr: Address, contact_addr: Optional[Address] = None):
        socket.setdefaulttimeout(RaftNode.RPC_TIMEOUT)
        # Self properties
        self.app:                   Any                 = application
        self.type:                  RaftNode.NodeType   = RaftNode.NodeType.FOLLOWER
        self.address:               Address             = addr
        self.cluster_leader_addr:   Optional[Address]   = None
        self.cluster_addr_list:     List[Address]       = []

        # Node properties
        self.currentTerm:           int                 = 0
        self.votedFor:              Optional[int]       = None
        self.log:                   List[LogEntry]      = [] # First idx is 0
        self.commitIdx:             int                 = 0
        self.lastApplied:           int                 = 0
        
        # Leader properties (Not None if leader, else None)
        self.nextIdx:               Optional[List[int]] = None
        self.matchIdx:              Optional[List[int]] = None

        self.__print_log("Server Start Time")
        if contact_addr is None:
            self.cluster_addr_list.append(self.address)
            self.__initialize_as_leader()
        else:
            self.__try_to_apply_membership(contact_addr)

    # Internal Raft Node methods
    def __print_log(self, text: str):
        print(f"[{self.address}] [{time.strftime('%H:%M:%S')}] {text}")

    def __initialize_as_leader(self):
        self.__print_log("Initialize as leader node..." )
        self.type                = RaftNode.NodeType.LEADER
        self.cluster_leader_addr = self.address
        if self.address in self.cluster_addr_list:
            self.cluster_addr_list.remove(self.address)
        
        request = {
            "cluster_leader_addr": self.address
        }
        # TODO : Inform to all node this is new leader
        self.heartbeat_thread = Thread(target=asyncio.run, daemon=True, args=[self.__leader_heartbeat()])
        self.heartbeat_thread.start()

    async def __leader_heartbeat(self):
        # TODO : Send periodic heartbeat
        while True:
            self.__print_log("[Leader] Sending heartbeat...")
            for target in self.cluster_addr_list:
                request = AppendEntriesRequest(target, "heartbeat", self.address)
                response = AppendEntriesResponse("success", target)
                if(response.term != "success"):
                    self.__print_log(f"Heartbeat to {target.ip}:{target.port} failed...")
                else:
                    self.__print_log(f"Heartbeat to {target.ip}:{target.port} success...")
            await asyncio.sleep(RaftNode.HEARTBEAT_INTERVAL)

    def __try_to_apply_membership(self, contact_addr: Address):
        redirected_addr = contact_addr
        request = AddressRequest(contact_addr, "apply_membership", self.address)
        response = MembershipResponse("redirected", contact_addr)
        while response.status != "success":
            redirected_addr = Address(response.address.ip, response.address.port)
            response        = asyncio.get_event_loop().run_until_complete(self.__send_request_async(request))
        self.log                 = response.log
        self.cluster_addr_list   = response.cluster_addr_list
        self.cluster_leader_addr = redirected_addr

    async def __send_request_async(self, req: Request) -> Any:
        node         = ServerProxy(f"http://{req.dest.ip}:{req.dest.port}")
        json_request = json.dumps(req, cls=RequestEncoder)
        rpc_function = getattr(node, req.func_name)
        result = rpc_function(json_request)
        response     = json.loads(result, cls=ResponseDecoder)
        self.__print_log(str(response))
        return response
    
    def __send_request(self, req: Request) -> Any:
        # Warning : This method is blocking
        node         = ServerProxy(f"http://{req.dest.ip}:{req.dest.port}")
        json_request = json.dumps(req, cls=RequestEncoder)
        rpc_function = getattr(node, req.func_name)
        response     = json.loads(rpc_function(json_request), cls=ResponseDecoder)
        self.__print_log(str(response))
        return response

    # Inter-node RPCs
    def heartbeat(self, json_request: str) -> str:
        # TODO : Implement heartbeat
        response = {
            "term": self.currentTerm + 1,
            "leaderId": self.cluster_leader_addr,
            "prevLogIdx": 0,
            "prevLogTerm": 0,
            "entries": [],
            "leaderCommit": 0
            #"heartbeat_response": "ack",
            #"address":            self.address
        }
        return json.dumps(response)
    
    async def __send_membership(self, request: Request):
        cluster_addr_send_list = self.cluster_addr_list.copy()
        cluster_addr_send_list.remove(request.body)
        while len(cluster_addr_send_list) > 0:
            tasks = []
            for i in range(len(cluster_addr_send_list)):
                cluster_addr = cluster_addr_send_list[i]
                request = AddressRequest(cluster_addr, "apply_membership", request.body)
                task = asyncio.ensure_future(self.__send_request_async(request))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
            
            cluster_addr_send_list_new = []
            for i in range(len(responses)):
                response = responses[i]
                if response.status != 'success':
                    cluster_addr_send_list_new.append(cluster_addr_send_list[i])
            cluster_addr_send_list = cluster_addr_send_list_new
    
    def apply_membership(self, json_request: str) -> str:
        request: AddressRequest = json.loads(json_request, cls=RequestDecoder)
        self.cluster_addr_list.append(request.body)
        if self.address == self.cluster_leader_addr and len(self.cluster_addr_list) > 0:
            thr = Thread(target=asyncio.run, daemon=True, args=[self.__send_membership(request)])
            thr.start()
        response = MembershipResponse("success", self.address, self.log, self.cluster_addr_list)
        print(response)
        return json.dumps(response, cls=ResponseEncoder)

    # Client RPCs
    def execute(self, json_request: str) -> str:
        request: ClientRequest = json.loads(json_request, cls=RequestDecoder)
        print("Request from Client\n", request, "\n")
        
        response = ClientRequestResponse(request.body.requestNumber, "success")
        print("Response to Client", response, "\n")
        # TODO : Implement execute
        self.log_replication(request)

        return json.dumps(response, cls=ResponseEncoder)
    
    def log_replication(self, cliReq: ClientRequest):
        print("Log Replication")

        log_entry = LogEntry(self.currentTerm, self.commitIdx, cliReq.dest, 
                             cliReq.body.command, cliReq.body.requestNumber, None)
        self.log.append(log_entry)

        entries: AppendEntriesBody = AppendEntriesBody(self.currentTerm, 0, self.commitIdx, 
                                                       self.lastApplied, self.log, self.commitIdx)

        print("Sending log replication request to all nodes...")
        print("AppendEntriesBody", entries, "\n")

        ack_array = [False] * len(self.cluster_addr_list)

        while sum(bool(x) for x in ack_array) < (len(self.cluster_addr_list) // 2) + 1:
            for i in range(len(self.cluster_addr_list)):
                if ack_array[i] == False:
                    request: AppendEntriesRequest = AppendEntriesRequest(self.cluster_addr_list[i], "receiver_replicate_log", entries)
                    response: AppendEntriesResponse = self.__send_request(request)
                    if response.success == True:
                        ack_array[i] = True

        print("Log replication success...")
        print("Committing log...")
        self.log[len(self.log) - 1].result = "Committed"
        print("Leader Log: ", self.log, "\n")


    def receiver_replicate_log(self, json_request: str):
        request: AppendEntriesRequest = json.loads(json_request, cls=RequestDecoder)
        print("Request from Leader:\n", request, "\n")

        self.log.append(request.body.entries[len(request.body.entries) - 1])
        print("Success append log to follower...")
        print("Follower Log: ", self.log, "\n")
        response = AppendEntriesResponse(self.currentTerm, True)

        return json.dumps(response, cls=ResponseEncoder)
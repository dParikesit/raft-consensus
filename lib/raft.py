import asyncio
import json
import socket
import time
from enum import Enum
from threading import Thread
from typing import AbstractSet, Any, List, MutableSet, Optional, Tuple
from xmlrpc.client import ServerProxy

from lib.struct.address import Address
from lib.struct.logEntry import LogEntry
from lib.struct.request.body import AppendEntriesBody, RequestVoteBody
from lib.struct.request.request import (AddressRequest, AppendEntriesRequest,
                                        ClientRequest, Request, RequestDecoder,
                                        RequestEncoder, StringRequest, RequestVoteRequest)
from lib.struct.response.response import (AppendEntriesResponse,
                                          ClientRequestResponse,
                                          MembershipResponse, Response,
                                          ResponseDecoder, ResponseEncoder, RequestVoteResponse)


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
        self.time_to_election:      int                 = 0

        # Node properties
        self.currentTerm:           int                 = 0
        self.votedFor:              Optional[Address]   = None
        self.log:                   List[LogEntry]      = [] # First idx is 0
        self.commitIdx:             int                 = -1
        self.lastApplied:           int                 = -1
        
        # Leader properties (Not None if leader, else None)
        self.nextIdx:               Optional[List[int]] = None
        self.matchIdx:              Optional[List[int]] = None

        self.__print_log("Server Start Time")
        if contact_addr is None:
            self.cluster_addr_list.append(self.address)
            self.__initialize_as_leader()
        else:
            self.__try_to_apply_membership(contact_addr)
            self.check_time_to_election = Thread(target=asyncio.run, daemon=True, args=[self.__check_time_to_election()])
            self.check_time_to_election.start()

    async def __check_time_to_election(self):
        #TODO: Break diganti dengan leader election
        while True:
            curr_time = round(time.time()*1000)
            if(self.time_to_election != 0 and curr_time - self.time_to_election > 2000):
                self.__print_log(f"Node {self.address.ip}:{self.address.port} want to be a leader...")
                break
            

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
            if(len(self.cluster_addr_list) > 0):
                isExcept = False
                self.lastApplied += 1
                self.nextIdx = len(self.log)
                ack_array = [False] * len(self.cluster_addr_list)

                while sum(bool(x) for x in ack_array) < (len(self.cluster_addr_list) // 2) + 1 and not isExcept:
                    if (self.nextIdx > 1):
                        self.nextIdx -= 1
                        prevLogIdx = self.log[self.nextIdx - 1].idx
                        prevLogTerm = self.log[self.nextIdx - 1].term
                    elif (self.nextIdx == 1 or self.nextIdx == 0):
                        self.nextIdx = 0
                        prevLogIdx = -1
                        prevLogTerm = -1

                    entries: AppendEntriesBody = AppendEntriesBody(self.currentTerm, 0, prevLogIdx, prevLogTerm, [], self.nextIdx)

                    for i in range(len(self.cluster_addr_list)):
                        if ack_array[i] == False:
                            try:
                                request: AppendEntriesRequest = AppendEntriesRequest(self.cluster_addr_list[i], "receiver_replicate_log", entries)
                                response: AppendEntriesResponse = self.__send_request(request)
                                if response.success == True:
                                    ack_array[i] = True
                                    self.__print_log(f"Heartbeat to {self.cluster_addr_list[i].ip}:{self.cluster_addr_list[i].port} success...")
                                else:
                                    self.__print_log(f"Heartbeat to {self.cluster_addr_list[i].ip}:{self.cluster_addr_list[i].port} failed...")
                            except:
                                self.__print_log(f"Heartbeat to {self.cluster_addr_list[i].ip}:{self.cluster_addr_list[i].port} died...")
                                self.__print_log(f"Node {self.cluster_addr_list[i].ip}:{self.cluster_addr_list[i].port} will be deleted...")
                                self.cluster_addr_list.pop(i)
                                isExcept = True
            await asyncio.sleep(RaftNode.HEARTBEAT_INTERVAL)
            '''
            for target in self.cluster_addr_list:

                request = AppendEntriesRequest(target, "heartbeat", self.address)
                response = AppendEntriesResponse("success", target)

                # kode adel
                entries: AppendEntriesBody = AppendEntriesBody(self.currentTerm, 0, prevLogIdx, prevLogTerm, self.log, self.nextIdx)

                print("Sending log replication request to all nodes...")
                print("AppendEntriesBody", entries, "\n")
                
                for i in range(len(self.cluster_addr_list)):
                    if ack_array[i] == False:
                        request: AppendEntriesRequest = AppendEntriesRequest(self.cluster_addr_list[i], "receiver_replicate_log", entries)
                        response: AppendEntriesResponse = self.__send_request(request)
                        if response.success == True:
                            self.__print_log(f"Heartbeat to {target.ip}:{target.port} success...")
                        else:
                            self.__print_log(f"Heartbeat to {target.ip}:{target.port} failed...")

                if(response.term != "success"):
                    self.__print_log(f"Heartbeat to {target.ip}:{target.port} failed...")
                else:
                    self.__print_log(f"Heartbeat to {target.ip}:{target.port} success...")
            '''

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

    async def election_start(self):
        if self.cluster_leader_addr:
            self.cluster_addr_list.append(self.cluster_leader_addr)
            self.cluster_leader_addr = None
        self.type = RaftNode.NodeType.CANDIDATE
        self.currentTerm += 1
        voteCount: int = 1
        tasks = []

        for addr in self.cluster_addr_list:
            if addr != self.address:
                request = RequestVoteRequest(addr, "election_vote", RequestVoteBody(self.currentTerm, self.address, len(self.log)+1, self.log[-1].term))
                task = asyncio.ensure_future(self.__send_request_async(request))
                tasks.append(task)
        responses: List[RequestVoteResponse] = await asyncio.gather(*tasks)
        for response in responses:
            if response.voteGranted:
                voteCount += 1
            else:
                if response.term > self.currentTerm:
                    self.type = RaftNode.NodeType.FOLLOWER # Bukan paling update
                    return
        
        if voteCount > ((len(self.log)//2) + 1):
            self.type = RaftNode.NodeType.LEADER
            self.cluster_leader_addr = self.address
            self.cluster_addr_list.remove(self.address)
    
    def election_vote(self, json_request: str) -> str:
        request: RequestVoteRequest = json.loads(json_request, cls=RequestDecoder)
        response = RequestVoteResponse(self.currentTerm, False)

        if self.votedFor == None or self.votedFor == request.body.candidateId:
            response.voteGranted = True
        else:
            return json.dumps(response, cls=ResponseEncoder)

        if request.body.lastLogTerm >= self.log[-1].term and request.body.lastLogIdx >= len(self.log) + 1:
            response.voteGranted = True
        else:
            return json.dumps(response, cls=ResponseEncoder)

        if self.currentTerm <= request.body.term:
            self.currentTerm = request.body.term
            response.term = self.currentTerm
            response.voteGranted = True
        else:
            return json.dumps(response, cls=ResponseEncoder)
            
        self.votedFor = request.body.candidateId
        return json.dumps(response, cls=ResponseEncoder)

    # Client RPCs
    def execute(self, json_request: str) -> str:
        request: ClientRequest = json.loads(json_request, cls=RequestDecoder)
        print("Request from Client\n", request, "\n")
        
        response = ClientRequestResponse(request.body.requestNumber, "success", "result")
        print("Response to Client", response, "\n")
        # TODO : Implement execute
        self.log_replication(request)
        print("LOG REPLICATION")
        # time.sleep(11)

        return json.dumps(response, cls=ResponseEncoder)
    
    def log_replication(self, cliReq: ClientRequest):
        print("Log Replication")

        log_entry = LogEntry(self.currentTerm, self.commitIdx + 1, cliReq.dest, 
                             cliReq.body.command, cliReq.body.requestNumber, None)
        self.log.append(log_entry)

        self.lastApplied += 1
        self.nextIdx = len(self.log)

        if (len(self.cluster_addr_list) > 0):
            ack_array = [False] * len(self.cluster_addr_list)

            while sum(bool(x) for x in ack_array) < (len(self.cluster_addr_list) // 2) + 1:
                if (self.nextIdx > 1):
                    self.nextIdx -= 1
                    prevLogIdx = self.log[self.nextIdx - 1].idx
                    prevLogTerm = self.log[self.nextIdx - 1].term
                elif (self.nextIdx == 1 or self.nextIdx == 0):
                    self.nextIdx = 0
                    prevLogIdx = -1
                    prevLogTerm = -1

                entries: AppendEntriesBody = AppendEntriesBody(self.currentTerm, 0, prevLogIdx, 
                                                            prevLogTerm, self.log, self.nextIdx)

                print("Sending log replication request to all nodes...")
                print("AppendEntriesBody", entries, "\n")
                
                for i in range(len(self.cluster_addr_list)):
                    if ack_array[i] == False:
                        request: AppendEntriesRequest = AppendEntriesRequest(self.cluster_addr_list[i], "receiver_replicate_log", entries)
                        response: AppendEntriesResponse = self.__send_request(request)
                        if response.success == True:
                            ack_array[i] = True

            print("Log replication success...")
            print("Committing log...")
            self.log[len(self.log) - 1].result = "Committed"
            self.commitIdx += 1
            
            print("Leader Log: ", self.log, "\n")
            print("Sending response to client...")

            entries: AppendEntriesBody = AppendEntriesBody(self.currentTerm, 0, None, None, self.log, self.commitIdx)

            for i in range(len(self.cluster_addr_list)):
                if ack_array[i] == True:
                    request: AppendEntriesRequest = AppendEntriesRequest(self.cluster_addr_list[i], "receiver_commit_log", entries)
                    response: AppendEntriesResponse = self.__send_request(request)


    def receiver_replicate_log(self, json_request: str):
        print("Receiver replicate log...")
        self.time_to_election = round(time.time()*1000)
        request: AppendEntriesRequest = json.loads(json_request, cls=RequestDecoder)
        if len(self.log) == 0:
            prevLogIdx = -1
            prevLogTerm = -1
        else:
            #print("Log: ", self.log, "\n")
            prevLogIdx = self.log[len(self.log) - 1]['idx']
            prevLogTerm = self.log[len(self.log) - 1]['term']
        print(request.body.term, self.currentTerm)
        if(request.body.term > self.currentTerm):
            self.currentTerm = request.body.term
            self.votedFor = None
        elif (request.body.term == self.currentTerm):
            if (prevLogIdx == request.body.prevLogIdx and prevLogTerm == request.body.prevLogTerm):
                print("Request from Leader:\n", request, "\n")

                for i in range(request.body.leaderCommit, len(request.body.entries)):
                    self.log.append(request.body.entries[i])
                    self.lastApplied += 1
                    if (request.body.entries[i]['result'] == "Committed"):
                        self.commitIdx += 1
                         
                print("Success append log to follower...")
                print("Follower Log: ", self.log, "\n")

                response = AppendEntriesResponse(self.currentTerm, True)
                return json.dumps(response, cls=ResponseEncoder)
            else:
                response = AppendEntriesResponse(self.currentTerm, False)
                return json.dumps(response, cls=ResponseEncoder)
        else:
            response = AppendEntriesResponse(self.currentTerm, False)
            return json.dumps(response, cls=ResponseEncoder)
    
    def receiver_commit_log(self, json_request: str):
        print("Receiver commit log...")
        request: AppendEntriesRequest = json.loads(json_request, cls=RequestDecoder)

        self.log[request.body.leaderCommit]['result'] = "Committed"
        self.commitIdx = request.body.leaderCommit

        print("Committing log...")
        print("Follower Log: ", self.log, "\n")

        response = AppendEntriesResponse(self.currentTerm, True)
        return json.dumps(response, cls=ResponseEncoder)
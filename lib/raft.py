import json
import socket
import time
from math import ceil
from enum import Enum
from threading import Thread
from typing import AbstractSet, Any, List, MutableSet, Optional, Tuple, Dict
from xmlrpc.client import ServerProxy
from lib.app import MessageQueue
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio

from lib.timer.CountdownTimer import CountdownTimer
from lib.struct.address import Address
from lib.struct.logEntry import LogEntry
from lib.struct.request.body import (
    AppendEntriesBody,
    AppendEntriesMembershipBody,
    RequestVoteBody
)
from lib.struct.request.request import (
    AddressRequest,
    AppendEntriesRequest,
    AppendEntriesMembershipRequest,
    ClientRequest,
    Request,
    RequestDecoder,
    RequestEncoder,
    RequestVoteRequest,
    StringRequest,
    ConfigChangeRequest
)
from lib.struct.response.response import (
    AppendEntriesResponse,
    ClientRedirectResponse,
    ClientRequestLogResponse,
    ClientRequestResponse,
    MembershipResponse,
    RequestVoteResponse,
    Response,
    ResponseDecoder,
    ResponseEncoder,
    ConfigChangeResponse
)


class RaftNode:
    HEARTBEAT_INTERVAL   = 2
    ELECTION_TIMEOUT_MIN = 8.0
    ELECTION_TIMEOUT_MAX = 10.0
    RPC_TIMEOUT          = 1

    class NodeType(Enum):
        LEADER    = 1
        CANDIDATE = 2
        FOLLOWER  = 3

    def __init__(self, application: MessageQueue, addr: Address, contact_addr: Optional[Address] = None):
        socket.setdefaulttimeout(RaftNode.RPC_TIMEOUT)
        # Self properties
        self.app:                   MessageQueue        = application
        self.type:                  RaftNode.NodeType   = RaftNode.NodeType.FOLLOWER
        self.address:               Address             = addr
        self.cluster_leader_addr:   Optional[Address]   = None
        self.cluster_addr_list:     List[Address]       = []

        # Node properties
        self.currentTerm:           int                 = 0
        self.votedFor:              Optional[Address]   = None
        self.log:                   List[LogEntry]      = [] # First idx is 0
        self.commitIdx:             int                 = -1
        self.lastApplied:           int                 = -1

        # Leader properties (Not None if leader, else None)
        # self.nextIdx:               Optional[List[int]] = None
        self.nextIdx:               Dict[int, int]  = {}
        self.matchIdx:              Dict[int, int]  = {}
        self.beatTimer:             CountdownTimer      = CountdownTimer(self.log_replication, interval=RaftNode.HEARTBEAT_INTERVAL, repeat=True)

        # Follower properties
        self.cdTimer:               CountdownTimer      = CountdownTimer(self.election_start, intervalMin=RaftNode.ELECTION_TIMEOUT_MIN, intervalMax=RaftNode.ELECTION_TIMEOUT_MAX)

        self.__print_log("Server Start Time")
        if contact_addr is None:
            self.cluster_addr_list.append(self.address)
            self.__initialize_as_leader()
        else:
            self.__try_to_apply_membership(contact_addr)
            self.cdTimer.start()

    # Internal Raft Node methods
    def __print_log(self, text: str):
        print(f"[{self.address}] [{time.strftime('%H:%M:%S')}] {text}")

    def __initialize_as_leader(self):
        self.__print_log("Initialize as leader node..." )
        self.type                = RaftNode.NodeType.LEADER
        self.cluster_leader_addr = self.address
        if self.address in self.cluster_addr_list:
            self.cluster_addr_list.remove(self.address)
        self.beatTimer.start()

    def __try_to_apply_membership(self, contact_addr: Address):
        redirected_addr = contact_addr
        request = AddressRequest(contact_addr, "apply_membership", self.address)
        response = MembershipResponse("redirected", contact_addr)
        while response.status != "success":
            request         = AddressRequest(response.leader, "apply_membership", self.address)
            response        = self.__retry_send_request(request)
        self.cluster_leader_addr = response.leader
        self.cluster_addr_list   = response.cluster_addr_list
        print("Join successful")
    
    def __send_request(self, req: Request) -> Any:
        node         = ServerProxy(f"http://{req.dest.ip}:{req.dest.port}")
        json_request = json.dumps(req, cls=RequestEncoder)
        rpc_function = getattr(node, req.func_name)
        result      = rpc_function(json_request)
        response     = json.loads(result, cls=ResponseDecoder)
        self.__print_log(str(response))
        return response
        # try:
        #     node         = ServerProxy(f"http://{req.dest.ip}:{req.dest.port}")
        #     json_request = json.dumps(req, cls=RequestEncoder)
        #     rpc_function = getattr(node, req.func_name)
        #     result      = rpc_function(json_request)
        #     response     = json.loads(result, cls=ResponseDecoder)
        #     self.__print_log(str(response))
        #     return response
        # except:
        #     self.__print_log(f"{req.type} failed")
        #     return None
    
    def __retry_send_request(self, req: Request) -> Any:
        response = None
        while not response:
            try:
                node         = ServerProxy(f"http://{req.dest.ip}:{req.dest.port}")
                json_request = json.dumps(req, cls=RequestEncoder)
                rpc_function = getattr(node, req.func_name)
                result      = rpc_function(json_request)
                response     = json.loads(result, cls=ResponseDecoder)
                self.__print_log(str(response))
                return response
            except:
                self.__print_log(f"{req.type} failed")

    # Inter-node RPCs
    def apply_membership(self, json_request: str) -> str:
        request: AddressRequest = json.loads(json_request, cls=RequestDecoder)

        if request.body in self.cluster_addr_list:
            response = MembershipResponse("success", self.address, self.cluster_addr_list)
            return json.dumps(response, cls=ResponseEncoder)

        if self.cluster_leader_addr and self.address != self.cluster_leader_addr:
            response = MembershipResponse("failed", self.cluster_leader_addr)
            return json.dumps(response, cls=ResponseEncoder)
        
        print("Apply membership for node", request.body)

        self.cluster_addr_list.append(request.body)

        with ThreadPoolExecutor() as executor:
            futures = []
            for addr in self.cluster_addr_list:
                if addr!=request.body:
                    futures.append(executor.submit(self.__retry_send_request, ConfigChangeRequest(addr, "update_addr_list", self.cluster_addr_list + [request.body])))
            for future in as_completed(futures):
                res: ConfigChangeResponse | None = future.result()
                if res and res.success:
                    self.__print_log(f"{res.dest} has gotten the message")
        
        self.nextIdx[request.body.port] = 0
        self.matchIdx[request.body.port] = -1

        response = MembershipResponse("success", self.address, self.cluster_addr_list)
        return json.dumps(response, cls=ResponseEncoder)
    
    def update_addr_list(self, json_request:str) -> str:
        self.cdTimer.reset()
        request: ConfigChangeRequest = json.loads(json_request, cls=RequestDecoder)
        self.cluster_addr_list = request.body

        response = ConfigChangeResponse(request.dest, True)
        return json.dumps(response, cls=ResponseEncoder)
    
    def election_start(self):
        if self.cluster_leader_addr:
            self.cluster_addr_list.append(self.cluster_leader_addr)
            self.cluster_leader_addr = None
        self.type = RaftNode.NodeType.CANDIDATE
        self.currentTerm += 1
        voteCount: int = 1

        with ThreadPoolExecutor() as executor:
            futures = []
            for addr in self.cluster_addr_list:
                if addr != self.address:
                    request = RequestVoteRequest(addr, "election_vote", RequestVoteBody(self.currentTerm, self.address, len(self.log), self.log[-1].term if len(self.log)>0 else -1))
                    futures.append(executor.submit(self.__send_request, request))
            for future in as_completed(futures):
                res: RequestVoteResponse | None = future.result()
                if res:
                    if res.voteGranted:
                        voteCount += 1
                    else:
                        if res.term > self.currentTerm:
                            self.type = RaftNode.NodeType.FOLLOWER # Bukan paling update
                            return

        print("vote count",voteCount, self.currentTerm, (ceil(len(self.cluster_addr_list)/2) + 1))
        if voteCount >= (ceil(len(self.cluster_addr_list)/2) + 1):
            self.type = RaftNode.NodeType.LEADER
            self.cluster_leader_addr = self.address
            self.cluster_addr_list.remove(self.address)
            for addr in self.cluster_addr_list:
                self.nextIdx[addr.port] = 0
                self.matchIdx[addr.port] = -1
        else:
            self.type = RaftNode.NodeType.FOLLOWER
            self.cdTimer.reset()
    
    def election_vote(self, json_request: str) -> str:
        request: RequestVoteRequest = json.loads(json_request, cls=RequestDecoder)
        response = RequestVoteResponse(self.currentTerm, False)

        if self.votedFor == None or self.votedFor == request.body.candidateId:
            response.voteGranted = True
        else:
            return json.dumps(response, cls=ResponseEncoder)

        if len(self.log)==0 or (request.body.lastLogTerm >= self.log[-1].term and request.body.lastLogIdx >= len(self.log)):
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
        self.cdTimer.reset()
        return json.dumps(response, cls=ResponseEncoder)

    # Client RPCs
    def execute(self, json_request: str) -> str:
        request: ClientRequest = json.loads(json_request, cls=RequestDecoder)
        print("Request from Client\n", request, "\n")

        # Check leader or follower
        if self.cluster_leader_addr is not None:
            if self.address == self.cluster_leader_addr:
                response = ClientRequestResponse(request.body.requestNumber, "success", "result")
                print("Response to Client", response, "\n")
                log_entry = LogEntry(self.currentTerm, True, request.body.clientID, request.body.command, request.body.requestNumber, None)
                self.log.append(log_entry)
                self.log_replication()
                print("LOG REPLICATION")
                # time.sleep(11)
            else:
                response = ClientRedirectResponse("Redirect", self.cluster_leader_addr)
                print("Response to Client", response, "\n")
        else:
            response = ClientRedirectResponse("No Leader", None)
            print("Response to Client", response, "\n")

        return json.dumps(response, cls=ResponseEncoder)

    def request_log(self, json_request: str) -> str:
        request: ClientRequest = json.loads(json_request, cls=RequestDecoder)
        print("Request from Client\n", request, "\n")

        # Check leader or follower
        if self.cluster_leader_addr is not None:
            if self.address == self.cluster_leader_addr:
                response = ClientRequestLogResponse("success", request.body.requestNumber, self.log)
                print("Response to Client", response, "\n")
            else:
                response = ClientRedirectResponse("Redirect", self.cluster_leader_addr)
                print("Response to Client", response, "\n")
        else:
            response = ClientRedirectResponse("No Leader", None)
            print("Response to Client", response, "\n")

        return json.dumps(response, cls=ResponseEncoder)

    def log_replication(self):
        print("Log Replication")
        self.beatTimer.reset()

        if len(self.cluster_addr_list)>0:
            # Check whether commit index can be increased.
            if len(self.log)>0:
                newCommitIdx = 0
                for idx in range(len(self.log)-1, self.commitIdx, -1):
                    count = len(dict(filter(lambda val: val[1]-1>= idx, self.nextIdx.items())))
                    if count >= ceil((len(self.cluster_addr_list)+1)/2) + 1:
                        newCommitIdx = idx
                        break
                for idx in range(self.commitIdx, newCommitIdx+1):
                    if self.log[idx].operation.startswith('enqueue') or self.log[idx].operation.startswith('dequeue'):
                        self.commit_entry(idx)
                        self.lastApplied +=1
                self.commitIdx = newCommitIdx
            
            # Send append entries that append AND commit logs. Remember that not all server will return (caused of network problem), hence the not all log and commit index in follower will be updated

            with ThreadPoolExecutor() as executor:
                futures = [executor.submit(self.__send_request, AppendEntriesRequest(addr, "receiver_log_replication", AppendEntriesBody(self.currentTerm, self.address, self.nextIdx[addr.port]-1, 0 if len(self.log)<=1 else self.log[self.nextIdx[addr.port]-1].term, self.log[self.nextIdx[addr.port]:], self.commitIdx))) for addr in self.cluster_addr_list]
                for future in as_completed(futures):
                    res: AppendEntriesResponse | None = future.result()
                    if res:
                        if res.term <= self.currentTerm:
                            self.currentTerm=res.term
                            if res.dest:
                                if res.success:
                                    self.nextIdx[res.dest.port] = len(self.log)
                                    self.matchIdx[res.dest.port] = self.commitIdx
                                else:
                                    self.nextIdx[res.dest.port] = 0
                                    self.matchIdx[res.dest.port] = -1
                        else:
                            self.type = RaftNode.NodeType.FOLLOWER

    def receiver_log_replication(self, json_request: str) -> str:
        print("Receiver replicate log...")
        self.cdTimer.reset()
        request: AppendEntriesRequest = json.loads(json_request, cls=RequestDecoder)
        response: AppendEntriesResponse = AppendEntriesResponse(request.dest, self.currentTerm, False)

        if(request.body.term < self.currentTerm):
            # Term is greater than leader, this indicates this node is more updated
            return json.dumps(response, cls=ResponseEncoder)
        
        if(request.body.prevLogIdx >=0 and self.log[request.body.prevLogIdx].term != request.body.term):
            # Incorrect previous log, reset log and MQ
            self.log = []
            self.app.clear()
            return json.dumps(response, cls=ResponseEncoder)
        
        if(request.body.prevLogIdx >=0 and request.body.prevLogIdx < len(self.log)-1):
            # Delete unused log (different tail than leader)
            for idx in range(len(self.log)-1, request.body.prevLogIdx):
                self.undo_entry(idx)
            del self.log[request.body.prevLogIdx+1:]
        
        # Append entries
        self.log += request.body.entries
        response.success = True

        # Commit until leader's commitIdx
        if request.body.leaderCommit>=0:
            for idx in range(self.commitIdx, request.body.leaderCommit+1):
                self.commit_entry(idx)
                self.lastApplied +=1
            self.commitIdx = request.body.leaderCommit

        return json.dumps(response, cls=ResponseEncoder)

    def commit_entry(self, idx: int):
        if(idx != 0):
            # Loop dari (idx - 1) sampe 0
            counter = idx
            while counter >= 0:
                if(self.log[idx].clientId == self.log[counter].clientId and self.log[idx].reqNum == self.log[counter].reqNum and self.log[idx].operation == self.log[counter].operation):
                    # clientID, reqNum, operation sama -> duplicate -> update result, don't apply
                    self.log[idx].result = self.log[counter].result
                    break
                else:
                    counter -= 1

            if (counter < 0):
                # No duplicate
                self.app.apply(self.log[idx].operation)

        else:
            # First log, no duplicate possible
            self.app.apply(self.log[idx].operation)
    
    def undo_entry(self, idx: int):
        if(idx != 0):
            # Loop dari (idx - 1) sampe 0
            counter = idx
            while counter >= 0:
                if(self.log[idx].clientId == self.log[counter].clientId and self.log[idx].reqNum == self.log[counter].reqNum and self.log[idx].operation == self.log[counter].operation):
                    # clientID, reqNum, operation sama -> duplicate -> delete log, don't pop or prepend
                    break
                else:
                    counter -= 1

            if (counter < 0):
                # No duplicate
                if self.log[idx].result:
                    if(self.log[idx].operation == "dequeue"):
                        self.app.prepend(self.log[idx].result) # type: ignore
                    else:
                        self.app.pop()

        else:
            # First log, no duplicate possible
            if self.log[idx].result:
                if(self.log[idx].operation == "dequeue"):
                    self.app.prepend(self.log[idx].result) # type: ignore
                else:
                    self.app.pop()
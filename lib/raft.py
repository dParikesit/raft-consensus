import asyncio
import json
import socket
import time
from enum import Enum
from threading import Thread
from typing import AbstractSet, Any, List, MutableSet, Optional, Tuple
from xmlrpc.client import ServerProxy

from lib.timer.CountdownTimer import CountdownTimer
from lib.struct.address import Address
from lib.struct.logEntry import LogEntry
from lib.struct.request.body import AppendEntriesBody, RequestVoteBody
from lib.struct.request.request import (
    AddressRequest,
    AppendEntriesRequest,
    ClientRequest,
    Request,
    RequestDecoder,
    RequestEncoder,
    RequestVoteRequest,
    StringRequest,
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
)


class RaftNode:
    HEARTBEAT_INTERVAL   = 1
    ELECTION_TIMEOUT_MIN = 2.0
    ELECTION_TIMEOUT_MAX = 3.0
    RPC_TIMEOUT          = 0.5

    class NodeType(Enum):
        LEADER    = 1
        CANDIDATE = 2
        FOLLOWER  = 3

    def __init__(self, application: Any, addr: Address, contact_addr: Optional[Address] = None):
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
        # self.nextIdx:               Optional[List[int]] = None
        self.nextIdx:               Optional[dict[int]] = {'nextIdx': None, 'prevLogIdx': None, 'prevLogTerm': None}
        self.matchIdx:              Optional[List[int]] = None
        self.nodeData:              Optional[dict[Address]] = {}

        # Follower properties
        self.cdTimer:               CountdownTimer      = CountdownTimer(RaftNode.ELECTION_TIMEOUT_MIN, RaftNode.ELECTION_TIMEOUT_MAX, self.election_start)

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
                for i in range(len(self.cluster_addr_list)):
                    '''if(self.nextIdx['nextIdx'] == None):
                        self.nextIdx['nextIdx'] = 0
                    if (self.nextIdx['nextIdx'] > 1):
                        # self.nextIdx['nextIdx'] -= 1
                        self.nextIdx['prevLogIdx'] = self.log[self.nextIdx['nextIdx'] - 1].idx
                        self.nextIdx['prevLogTerm'] = self.log[self.nextIdx['nextIdx'] - 1].term
                    elif (self.nextIdx['nextIdx'] == 1 or self.nextIdx['nextIdx'] == 0):
                        self.nextIdx['nextIdx'] = 0
                        self.nextIdx['prevLogIdx'] = -1
                        self.nextIdx['prevLogTerm'] = -1'''
                    if(self.cluster_addr_list[i].port not in self.nodeData):
                        self.nodeData[self.cluster_addr_list[i].port] = [0, -1, -1]
                    entries: AppendEntriesBody = AppendEntriesBody(self.currentTerm, 0, self.nodeData[self.cluster_addr_list[i].port][1], self.nodeData[self.cluster_addr_list[i].port][2], [], self.nodeData[self.cluster_addr_list[i].port][0])
                    try:
                        request: AppendEntriesRequest = AppendEntriesRequest(self.cluster_addr_list[i], "heartbeat", entries)
                        response: AppendEntriesResponse = self.__send_request(request)
                        if response.success == True:
                            self.__print_log(f"Heartbeat to {self.cluster_addr_list[i].ip}:{self.cluster_addr_list[i].port} success...")
                        else:
                            self.__print_log(f"Heartbeat to {self.cluster_addr_list[i].ip}:{self.cluster_addr_list[i].port} failed...")
                    except Exception as e:
                        self.__print_log(f"Error: {e}")
                        self.__print_log(f"Heartbeat to {self.cluster_addr_list[i].ip}:{self.cluster_addr_list[i].port} died...")
                        self.__print_log(f"Node {self.cluster_addr_list[i].ip}:{self.cluster_addr_list[i].port} will be deleted...")
                        self.cluster_addr_list.pop(i)


            await asyncio.sleep(RaftNode.HEARTBEAT_INTERVAL)
            '''
            for target in self.cluster_addr_list:

                request = AppendEntriesRequest(target, "heartbeat", self.address)
                response = AppendEntriesResponse("success", target)

                # kode adel
                entries: AppendEntriesBody = AppendEntriesBody(self.currentTerm, 0, self.nextIdx['prevLogIdx'], self.nextIdx['prevLogTerm'], self.log, self.nextIdx)

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
        try:
            node         = ServerProxy(f"http://{req.dest.ip}:{req.dest.port}")
            json_request = json.dumps(req, cls=RequestEncoder)
            rpc_function = getattr(node, req.func_name)
            result = rpc_function(json_request)
            response     = json.loads(result, cls=ResponseDecoder)
            self.__print_log(str(response))
            return response
        except Exception as error:
            # print("Exception occured")
            print(error)
            return None

    
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
        self.cdTimer.reset()
        request: AppendEntriesRequest = json.loads(json_request, cls=RequestDecoder)

        if not self.cluster_leader_addr:
            self.cluster_leader_addr = request.body.leaderId

        if self.cluster_leader_addr and request.body.leaderId != self.cluster_leader_addr:
            self.cluster_addr_list.append(self.cluster_leader_addr)
            self.cluster_leader_addr = request.body.leaderId

        print("JSON REQ: ", json_request)
        '''response = {
            "term": self.currentTerm + 1,
            "leaderId": self.cluster_leader_addr,
            "self.nextIdx['prevLogIdx']": self.nodeData[self.cluster_leader_addr.port][1],
            "self.nextIdx['prevLogTerm']": self.nodeData[self.cluster_leader_addr.port][2],
            "entries": [],
            "leaderCommit": self.nodeData[self.cluster_leader_addr.port][0],

            "heartbeat_response": "ack",
            "address":            self.address
        }'''
        response = AppendEntriesResponse(self.currentTerm, True)
        return json.dumps(response, cls=ResponseEncoder)
    
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
                request = RequestVoteRequest(addr, "election_vote", RequestVoteBody(self.currentTerm, self.address, len(self.log), self.log[-1].term if len(self.log)>0 else -1))
                task = asyncio.ensure_future(self.__send_request_async(request))
                tasks.append(task)
        responses: List[RequestVoteResponse | None] = await asyncio.gather(*tasks)
        print(responses)
        for response in responses:
            if response != None:
                if response.voteGranted:
                    voteCount += 1
                else:
                    if response.term > self.currentTerm:
                        self.type = RaftNode.NodeType.FOLLOWER # Bukan paling update
                        return
        print("vote count",voteCount, self.currentTerm, ((len(self.cluster_addr_list)//2) + 1))
        if voteCount >= ((len(self.cluster_addr_list)//2) + 1):
            self.type = RaftNode.NodeType.LEADER
            self.cluster_leader_addr = self.address
            self.cluster_addr_list.remove(self.address)
            self.heartbeat_thread = Thread(target=asyncio.run, daemon=True, args=[self.__leader_heartbeat()])
            self.heartbeat_thread.start()
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
                self.log_replication(request)
                print("LOG REPLICATION")
                # time.sleep(11)
            else:
                response = ClientRedirectResponse("Redirect", self.cluster_leader_addr)
                print("Response to Client", response, "\n")
        else:
            response = ClientRedirectResponse("No Leader", None)
            print("Response to Client", response, "\n")

        return json.dumps(response, cls=ResponseEncoder)

    def request_log(self, json_request: str):
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

    def log_replication(self, cliReq: ClientRequest):
        print("Log Replication")

        log_entry = LogEntry(self.currentTerm, self.commitIdx + 1, cliReq.body.clientID, 
                             cliReq.body.command, cliReq.body.requestNumber, None)
        self.log.append(log_entry)

        self.lastApplied += 1
        # self.nextIdx['nextIdx'] = len(self.log)
        self.nextIdx['nextIdx'] = len(self.log)

        if (len(self.cluster_addr_list) > 0):
            ack_array = [False] * len(self.cluster_addr_list)

            while sum(bool(x) for x in ack_array) < (len(self.cluster_addr_list) // 2) + 1:
                if (self.nextIdx['nextIdx'] > 1):
                    self.nextIdx['nextIdx'] -= 1
                    self.nextIdx['prevLogIdx'] = self.log[self.nextIdx['nextIdx'] - 1].idx
                    self.nextIdx['prevLogTerm'] = self.log[self.nextIdx['nextIdx'] - 1].term
                elif (self.nextIdx['nextIdx'] == 1 or self.nextIdx['nextIdx'] == 0):
                    self.nextIdx['nextIdx'] = 0
                    self.nextIdx['prevLogIdx'] = -1
                    self.nextIdx['prevLogTerm'] = -1

                entries: AppendEntriesBody = AppendEntriesBody(self.currentTerm, self.address, self.nextIdx['prevLogIdx'], 
                                                            self.nextIdx['prevLogTerm'], self.log, self.nextIdx['nextIdx'])

                print("Sending log replication request to all nodes...")
                print("AppendEntriesBody", entries, "\n")
                
                for i in range(len(self.cluster_addr_list)):
                    if ack_array[i] == False:
                        request: AppendEntriesRequest = AppendEntriesRequest(self.cluster_addr_list[i], "receiver_replicate_log", entries)
                        response: AppendEntriesResponse = self.__send_request(request)
                        if response.success == True:
                            ack_array[i] = True
                            self.nodeData[self.cluster_addr_list[i].port] = [self.nextIdx['nextIdx'], self.nextIdx['prevLogIdx'], self.nextIdx['prevLogTerm']]
                            print("Self_node_data: ", self.nodeData)

            print("Log replication success...")
            print("Committing log...")
            
            self.update_commit_log()
            self.commitIdx += 1
            
            print("Leader Log: ", self.log, "\n")
            print("Sending response to client...")

            entries: AppendEntriesBody = AppendEntriesBody(self.currentTerm, 0, None, None, self.log, self.commitIdx)

            for i in range(len(self.cluster_addr_list)):
                if ack_array[i] == True:
                    request: AppendEntriesRequest = AppendEntriesRequest(self.cluster_addr_list[i], "receiver_commit_log", entries)
                    response: AppendEntriesResponse = self.__send_request(request)
        
        else:
            print("Log replication success...")
            print("Committing log...")
            self.update_commit_log()
            self.commitIdx += 1
            print("Leader Log: ", self.log, "\n")



    def receiver_replicate_log(self, json_request: str):
        print("Receiver replicate log...")
        self.cdTimer.reset()
        request: AppendEntriesRequest = json.loads(json_request, cls=RequestDecoder)
        if len(self.log) == 0:
            self.nextIdx['prevLogIdx'] = -1
            self.nextIdx['prevLogTerm'] = -1
        else:
            #print("Log: ", self.log, "\n")
            self.nextIdx['prevLogIdx'] = self.log[len(self.log) - 1].idx
            self.nextIdx['prevLogTerm'] = self.log[len(self.log) - 1].term

        if(request.body.term > self.currentTerm):
            self.currentTerm = request.body.term
            self.votedFor = None
        elif (request.body.term == self.currentTerm):
            if (self.nextIdx['prevLogIdx'] == request.body.self.nextIdx['prevLogIdx'] and self.nextIdx['prevLogTerm'] == request.body.self.nextIdx['prevLogTerm']):
                print("Request from Leader:\n", request, "\n")

                for i in range(request.body.leaderCommit, len(request.body.entries)):
                    self.log.append(request.body.entries[i])
                    self.lastApplied += 1
                    if (request.body.entries[i].result != None):
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

        self.log[request.body.leaderCommit].result = "Committed"
        self.commitIdx = request.body.leaderCommit

        print("Committing log...")
        self.update_commit_log()
        print("Follower Log: ", self.log, "\n")

        response = AppendEntriesResponse(self.currentTerm, True)
        return json.dumps(response, cls=ResponseEncoder)

    def update_commit_log(self):
        results = []
        for i in range(len(self.log) - 1):
            if self.log[i].operation == "dequeue":
                if (len(results) > 0):
                    results.pop(0)
            else:
                results.append(self.log[i].operation[8:-1])
        if (self.log[len(self.log) - 1].operation == "dequeue"):
            if len(results) > 0:
                self.log[len(self.log) -1].result = results[0]
            else:  
                self.log[len(self.log) -1].result = "-1"
        else:
            self.log[len(self.log) -1].result = len(results) + 1

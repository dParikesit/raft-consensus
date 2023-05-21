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
    ELECTION_TIMEOUT_MIN = 8.0
    ELECTION_TIMEOUT_MAX = 10.0
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
        self.cluster_addr_new_list: List[Address]       = []
        self.time_to_election:      int                 = 0
        self.is__joint_consensus_running                = False

        # Node properties
        self.currentTerm:           int                 = 0
        self.votedFor:              Optional[Address]   = None
        self.log:                   List[LogEntry]      = [] # First idx is 0
        self.commitIdx:             int                 = -1
        self.lastApplied:           int                 = -1

        # Leader properties (Not None if leader, else None)
        # self.nextIdx:               Optional[List[int]] = None
        self.nextIdx:               Optional[dict[int]] = {}
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
            # self.cluster_addr_list = self.cluster_addr_new_list.copy()
            if(len(self.cluster_addr_list) > 0):
                for i in range(len(self.cluster_addr_list)):
                    if(self.cluster_addr_list[i].port not in self.nextIdx):
                        self.nextIdx[self.cluster_addr_list[i].port] = 0

                    nextIdx = self.nextIdx[self.cluster_addr_list[i].port]
                    if (nextIdx > 1):
                        prevLogIdx = self.log[len(self.log) - 1].idx
                        prevLogTerm = self.log[len(self.log) - 1].term
                    elif (nextIdx == 1 or nextIdx == 0):
                        prevLogIdx = -1
                        prevLogTerm = -1
                    currentTerm = self.currentTerm
                    entries: AppendEntriesBody = AppendEntriesBody(currentTerm, self.address, prevLogIdx, prevLogTerm, [], nextIdx)
                    try:
                        request: AppendEntriesRequest = AppendEntriesRequest(self.cluster_addr_list[i], "receiver_replicate_log", entries)
                        await asyncio.ensure_future(self.__send_request_async(request))
                        self.__print_log(f"Heartbeat to {self.cluster_addr_list[i].ip}:{self.cluster_addr_list[i].port} success...")
                    except Exception as e:
                        print(e)
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
        result      = rpc_function(json_request)
        response     = json.loads(result, cls=ResponseDecoder)
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

        response = AppendEntriesResponse(self.currentTerm, True)
        return json.dumps(response, cls=ResponseEncoder)
    

    
    async def __coldnew_log_sync(self, request: AddressRequest, cluster_addr_new_list: list):
        # Try to replicate Cold,new log on C_old
        log_entry = LogEntry(self.currentTerm, self.commitIdx+1, self.address, 
                             "Cold,new", 1, None)
        self.log.append(log_entry)
        self.lastApplied += 1
        
        if len(self.cluster_addr_list) > 0:
            self.nextIdx = len(self.log)
            cluster_addr_list_notack = self.cluster_addr_list.copy()
            count_success = 0
            while True:
                # cluster_addr_send_list_i_new = []
                if (self.nextIdx > 1):
                    self.nextIdx -= 1
                    prevLogIdx = self.log[self.nextIdx - 1].idx
                    prevLogTerm = self.log[self.nextIdx - 1].term
                elif (self.nextIdx == 1 or self.nextIdx == 0):
                    self.nextIdx = 0
                    prevLogIdx = -1
                    prevLogTerm = -1
                appendEntriesMembershipBody = AppendEntriesMembershipBody(self.currentTerm, 0, prevLogIdx, 
                                                                        prevLogTerm, self.log, self.nextIdx, cluster_addr_new_list)

                print("Sending Cold,new log to all Cold nodes...")
                tasks = []
                for cluster_addr in cluster_addr_list_notack:
                    request: AppendEntriesMembershipRequest = AppendEntriesMembershipRequest(
                        cluster_addr, "receiver_replicate_log_coldnew_conf", appendEntriesMembershipBody)
                    task = asyncio.ensure_future(self.__send_request_async(request))
                    task.append(task)
                responses: List[AppendEntriesResponse] = await asyncio.gather(*tasks)

                cluster_addr_list_notack_new = []
                for i in range(len(responses)):
                    response = responses[i]
                    if not response.success:
                        cluster_addr_list_notack_new.append(cluster_addr_list_notack[i])
                    else:
                        count_success += 1
                cluster_addr_list_notack = cluster_addr_list_notack_new
                if count_success >= len(self.cluster_addr_list) // 2: break
            print("Cold,new log replication to Cold success...")
            # Cold,new log committed on Cold    
            
        # C_new
        # Try to replicate Cold,new log on C_new 
        self.nextIdx = len(self.log)
        cluster_addr_new_list_notack = cluster_addr_new_list.copy()
        count_success = 0
        while True:
            if (self.nextIdx > 1):
                self.nextIdx -= 1
                prevLogIdx = self.log[self.nextIdx - 1].idx
                prevLogTerm = self.log[self.nextIdx - 1].term
            elif (self.nextIdx == 1 or self.nextIdx == 0):
                self.nextIdx = 0
                prevLogIdx = -1
                prevLogTerm = -1
            appendEntriesMembershipBody = AppendEntriesMembershipBody(self.currentTerm, 0, prevLogIdx, 
                                                                    prevLogTerm, self.log, self.nextIdx, cluster_addr_new_list)
            print("Sending Cold,new log to all Cnew nodes...")
            tasks = []
            for cluster_addr in cluster_addr_new_list_notack:
                request: AppendEntriesMembershipRequest = AppendEntriesMembershipRequest(
                        cluster_addr, "receiver_replicate_log_coldnew_conf", appendEntriesMembershipBody)
                task = asyncio.ensure_future(self.__send_request_async(request))
                tasks.append(task)
            responses: List[AppendEntriesResponse] = await asyncio.gather(*tasks)
            
            cluster_addr_new_list_notack_new = []
            count_success = 0
            for i in range(len(responses)):
                response = responses[i]
                if not response.success:
                    cluster_addr_new_list_notack_new.append(cluster_addr_new_list_notack[i])
                else:
                    count_success += 1
            cluster_addr_new_list_notack = cluster_addr_new_list_notack_new
            if count_success >= len(cluster_addr_new_list) // 2: break

        print("Cold,new log replication to Cnew success...")
        print("Committing log...")
        self.log[len(self.log)-1].result = "Committed"
        self.commitIdx += 1
        print("Cold,new log committed")
        print("Leader log: ", self.log, "\n")
        # Cold,new log committed on Cnew
    
    async def __cnew_log_sync(self, request: AddressRequest, cluster_addr_new_list: list):
        # Try to replicate Cnew log on C_new
        log_entry = LogEntry(self.currentTerm, self.commitIdx+1, self.address, 
                             "Cnew", 1, None)
        self.log.append(log_entry)
        self.nextIdx = len(self.log)
        self.lastApplied += 1
        
        len_cluster_addr_list_old = len(self.cluster_addr_list)
        cluster_addr_list_notack = self.cluster_addr_list.copy()
        cluster_addr_new_list_notack = cluster_addr_new_list.copy()
       
        count_success = 0
        while True:
            if (self.nextIdx > 1):
                self.nextIdx -= 1
                prevLogIdx = self.log[self.nextIdx - 1].idx
                prevLogTerm = self.log[self.nextIdx - 1].term
            elif (self.nextIdx == 1 or self.nextIdx == 0):
                self.nextIdx = 0
                prevLogIdx = -1
                prevLogTerm = -1
            appendEntriesMembershipBody = AppendEntriesMembershipBody(self.currentTerm, 0, prevLogIdx, 
                                                                    prevLogTerm, self.log, self.nextIdx, self.cluster_addr_list)
            print("Sending Cold,new log to all Cnew nodes...")

            count_success = 0
            cluster_addr_list_notack_new = []
            tasks = []
            for cluster_addr in cluster_addr_list_notack:
                request: AppendEntriesMembershipRequest = AppendEntriesMembershipRequest(
                        cluster_addr, "receiver_replicate_log_cnew_conf", appendEntriesMembershipBody)
                task = asyncio.ensure_future(self.__send_request_async(request))
                tasks.append(task)
            responses: List[AppendEntriesResponse] = await asyncio.gather(*tasks)
            for i in range(len(responses)):
                response = responses[i]
                if not response.success:
                    cluster_addr_list_notack_new.append(cluster_addr_list_notack[i])
                else:
                    count_success += 1
            
            cluster_addr_new_list_notack_new = []
            for cluster_addr in cluster_addr_new_list_notack:
                request: AppendEntriesMembershipRequest = AppendEntriesMembershipRequest(
                        cluster_addr, "receiver_replicate_log_cnew_conf", appendEntriesMembershipBody)
                response: AppendEntriesResponse = await self.__send_request_async(request)
                if not response.success:
                    cluster_addr_new_list_notack_new.append(cluster_addr)
                else:
                    self.cluster_addr_list.append(cluster_addr)
                    count_success += 1

            cluster_addr_new_list_notack = cluster_addr_new_list_notack_new
            cluster_addr_list_notack = cluster_addr_list_notack_new
            if count_success >= len_cluster_addr_list_old + len(cluster_addr_new_list) // 2: break

        print("Cnew log replication to Cnew success...")
        print("Committing log...")
        self.log[len(self.log)-1].result = "Committed"
        self.commitIdx += 1
        print("Cnew log committed")
        print("Leader log: ", self.log, "\n")
        # Cnew log committed on Cnew
        
    async def __joint_consensus(self, request: Request):
        print("Start Joint Consensus")
        await asyncio.sleep(5)
        print("Joint Consensus Running...")
        print("Cold:", self.cluster_addr_list)
        print("Cnew:", self.cluster_addr_new_list)
        
        cluster_addr_new_list = self.cluster_addr_new_list.copy()
        self.cluster_addr_new_list = []
        
        await self.__coldnew_log_sync(request, cluster_addr_new_list)
        await self.__cnew_log_sync(request, cluster_addr_new_list)
        self.is__joint_consensus_running = False

    def apply_membership(self, json_request: str) -> str:
        if self.address != self.cluster_leader_addr:
            response = MembershipResponse("failed", self.address, [], [])
            print(response)
            return json.dumps(response, cls=ResponseEncoder)
        request: AddressRequest = json.loads(json_request, cls=RequestDecoder)
        print("Apply membership for node", request.body)
        self.cluster_addr_new_list.append(request.body)
        
        if self.is__joint_consensus_running == False:
            self.is__joint_consensus_running = True
            thr = Thread(target=asyncio.run, daemon=True, args=[self.__joint_consensus(request)])
            thr.start()
            
        response = MembershipResponse("success", self.address, self.log, self.cluster_addr_list)
        print(response)
        return json.dumps(response, cls=ResponseEncoder)

    def receiver_replicate_log_coldnew_conf(self, json_request: str):
        print("Receiver replicate Cold,new log")
        self.cdTimer.reset()
        try:
            request: AppendEntriesMembershipRequest = json.loads(json_request, cls=RequestDecoder)
            bodyReq: AppendEntriesMembershipBody = request.body

            if len(self.log) == 0:
                prevLogIdx = -1
                prevLogTerm = -1
            else:
                #print("Log: ", self.log, "\n")
                prevLogIdx = self.log[len(self.log) - 1].idx
                prevLogTerm = self.log[len(self.log) - 1].term
            print(bodyReq.term, self.currentTerm)
            if(bodyReq.term > self.currentTerm):
                self.currentTerm = bodyReq.term
                self.votedFor = None
                response = AppendEntriesResponse(self.currentTerm, False)
                print(response)
                return json.dumps(response, cls=ResponseEncoder)
            elif (bodyReq.term == self.currentTerm):
                if (prevLogIdx == bodyReq.prevLogIdx and prevLogTerm == bodyReq.prevLogTerm):
                    print("Request from Leader:\n", request, "\n")
                    
                    for i in range(bodyReq.leaderCommit, len(bodyReq.entries)):
                        self.log.append(bodyReq.entries[i])
                        self.lastApplied += 1
                        if (bodyReq.entries[i]['result'] == "Committed"):
                            self.commitIdx += 1
                
                    print("Success append Cold,new log to follower...")
                    print("Follower Log: ", self.log, "\n")

                    self.cluster_addr_new_list = bodyReq.cluster_addr_list
                
                    print("Success to save Cold,new configuration...")
                    print("Follower old configuration:", self.cluster_addr_list)
                    print("Follower new configuration: ", self.cluster_addr_new_list)

                    response = AppendEntriesResponse(self.currentTerm, True)
                    print(response)
                    return json.dumps(response, cls=ResponseEncoder)
                else:
                    response = AppendEntriesResponse(self.currentTerm, False)
                    print(response)
                    return json.dumps(response, cls=ResponseEncoder)
            else:
                response = AppendEntriesResponse(self.currentTerm, False)
                print(response)
                return json.dumps(response, cls=ResponseEncoder)
        except Exception as e:
            print(e)
            response = AppendEntriesResponse(self.currentTerm, False)
            print(response)
            return json.dumps(response, cls=ResponseEncoder)
    
    def receiver_replicate_log_cnew_conf(self, json_request: str):
        print("Receiver replicate Cnew log")
        self.cdTimer.reset()
        try:
            request: AppendEntriesMembershipRequest = json.loads(json_request, cls=RequestDecoder)
            bodyReq: AppendEntriesMembershipBody = request.body
            if len(self.log) == 0:
                prevLogIdx = -1
                prevLogTerm = -1
            else:
                #print("Log: ", self.log, "\n")
                prevLogIdx = self.log[len(self.log) - 1]['idx']
                prevLogTerm = self.log[len(self.log) - 1]['term']
            print(bodyReq.term, self.currentTerm)
            if(bodyReq.term > self.currentTerm):
                self.currentTerm = bodyReq.term
                self.votedFor = None
                response = AppendEntriesResponse(self.currentTerm, False)
                print(response)
                return json.dumps(response, cls=ResponseEncoder)
            elif (bodyReq.term == self.currentTerm):
                if (prevLogIdx == bodyReq.prevLogIdx and prevLogTerm == bodyReq.prevLogTerm):
                    print("Request from Leader:\n", request, "\n")
                    
                    for i in range(bodyReq.leaderCommit, len(bodyReq.entries)):
                        self.log.append(bodyReq.entries[i])
                        self.lastApplied += 1
                        if (bodyReq.entries[i]['result'] == "Committed"):
                            self.commitIdx += 1

                    print("Success append Cnew log to follower...")
                    print("Follower Log: ", self.log, "\n")
            
                    self.cluster_addr_list = bodyReq.cluster_addr_list
                    self.cluster_addr_new_list = []
                    
                    print("Success to save Cnew configuration...")
                    print("Follower configuration: ", self.cluster_addr_list)

                    response = AppendEntriesResponse(self.currentTerm, True)
                    print(response)
                    return json.dumps(response, cls=ResponseEncoder)
                else:
                    response = AppendEntriesResponse(self.currentTerm, False)
                    print(response)
                    return json.dumps(response, cls=ResponseEncoder)
            else:
                response = AppendEntriesResponse(self.currentTerm, False)
                print(response)
                return json.dumps(response, cls=ResponseEncoder)
        except Exception as e:
            print(e)
            response = AppendEntriesResponse(self.currentTerm, False)
            print(response)
            return json.dumps(response, cls=ResponseEncoder)
    
    """
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
    """
    
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
        nextIdx = len(self.log)

        if (len(self.cluster_addr_list) > 0):
            ack_array = [False] * len(self.cluster_addr_list)

            while sum(bool(x) for x in ack_array) < (len(self.cluster_addr_list) // 2) + 1:
                if (nextIdx > 1):
                    nextIdx -= 1
                    prevLogIdx = self.log[nextIdx - 1].idx
                    prevLogTerm = self.log[nextIdx - 1].term
                elif (nextIdx == 1 or nextIdx == 0):
                    nextIdx = 0
                    prevLogIdx = -1
                    prevLogTerm = -1

                entries: AppendEntriesBody = AppendEntriesBody(self.currentTerm, self.address, prevLogIdx, 
                                                            prevLogTerm, self.log, nextIdx)

                print("Sending log replication request to all nodes...")
                print("AppendEntriesBody", entries, "\n")
                
                for i in range(len(self.cluster_addr_list)):
                    if ack_array[i] == False:
                        request: AppendEntriesRequest = AppendEntriesRequest(self.cluster_addr_list[i], "receiver_replicate_log", entries)
                        response: AppendEntriesResponse = self.__send_request(request)
                        if response.success == True:
                            ack_array[i] = True
                            self.nextIdx[self.cluster_addr_list[i].port] = nextIdx
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
        print("Request from Leader\n", request, "\n")
        if len(self.log) == 0:
            prevLogIdx = -1
            prevLogTerm = -1
        else:
            #print("Log: ", self.log, "\n")
            prevLogIdx = self.log[len(self.log) - 1].idx
            prevLogTerm = self.log[len(self.log) - 1].term
        print(request.body.term, self.currentTerm, prevLogIdx,  request.body.prevLogIdx, prevLogTerm, request.body.prevLogTerm)
        if(request.body.term > self.currentTerm):
            self.currentTerm = request.body.term
            self.votedFor = None
            response = AppendEntriesResponse(self.currentTerm, True)
            return json.dumps(response, cls=ResponseEncoder)
        elif (request.body.term == self.currentTerm):
            if(request.body.entries == []):
                response = AppendEntriesResponse(self.currentTerm, True)
                return json.dumps(response, cls=ResponseEncoder)
            if (prevLogIdx == request.body.prevLogIdx and prevLogTerm == request.body.prevLogTerm):
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

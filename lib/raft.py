import asyncio
import json
import socket
import time
from enum import Enum
from threading import Thread
from typing import AbstractSet, Any, List, MutableSet, Optional, Tuple, Dict
from xmlrpc.client import ServerProxy
from lib.app import MessageQueue

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

    def __init__(self, application: MessageQueue, addr: Address, contact_addr: Optional[Address] = None):
        socket.setdefaulttimeout(RaftNode.RPC_TIMEOUT)
        # Self properties
        self.app:                   MessageQueue        = application
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
        self.nextIdx:               Dict[Address, int] = {}
        self.matchIdx:              Dict[Address, int] = {}

        # Follower properties
        self.cdTimer:               CountdownTimer      = CountdownTimer(self.election_start, intervalMin=RaftNode.ELECTION_TIMEOUT_MIN, intervalMax=RaftNode.ELECTION_TIMEOUT_MAX )

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
        self.heartbeat_thread = Thread(target=asyncio.run, daemon=True, args=[self.__leader_heartbeat()])
        self.heartbeat_thread.start()

    async def __leader_heartbeat(self):
        while True:
            self.__print_log("[Leader] Sending heartbeat...")
            # self.cluster_addr_list = self.cluster_addr_new_list.copy()
            if(len(self.cluster_addr_list) > 0):
                for i in range(len(self.cluster_addr_list)):
                    if(self.cluster_addr_list[i] not in self.nextIdx):
                        self.nextIdx[self.cluster_addr_list[i]] = 0

                    nextIdx = self.nextIdx[self.cluster_addr_list[i]]
                    prevLogIdx = -1
                    prevLogTerm = -1

                    if (nextIdx > 1):
                        prevLogIdx = len(self.log)
                        prevLogTerm = self.log[len(self.log) - 1].term
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
            response.dest= req.dest
            self.__print_log(str(response))
            return response
        except Exception as error:
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
    
    
    async def __coldnew_log_sync(self, cluster_addr_new_list: list):
        # Try to replicate Cold,new log on C_old
        log_entry = LogEntry(self.currentTerm, False, str(self.address), 
                             "Cold,new", 1, None)
        self.log.append(log_entry)
        self.lastApplied += 1
        
        if len(self.cluster_addr_list) > 0:
            nextIdx = len(self.log)
            cluster_addr_list_notack = self.cluster_addr_list.copy()
            count_success = 0
            while True:
                nextIdx = 0
                prevLogIdx = -1
                prevLogTerm = -1

                if (nextIdx > 1):
                    nextIdx -= 1
                    prevLogIdx = nextIdx
                    prevLogTerm = self.log[nextIdx - 1].term

                entries: AppendEntriesMembershipBody = AppendEntriesMembershipBody(self.currentTerm, self.address, prevLogIdx, 
                                                                        prevLogTerm, self.log, nextIdx, cluster_addr_new_list)

                print("Sending Cold,new log to all Cold nodes...")
                tasks = []
                for cluster_addr in cluster_addr_list_notack:
                    request: AppendEntriesMembershipRequest = AppendEntriesMembershipRequest(
                        cluster_addr, "receiver_replicate_log_coldnew_conf", entries)
                    task = asyncio.ensure_future(self.__send_request_async(request))
                    tasks.append(task)
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
        nextIdx = len(self.log)
        cluster_addr_new_list_notack = cluster_addr_new_list.copy()
        count_success = 0
        while True:
            nextIdx = 0
            prevLogIdx = -1
            prevLogTerm = -1

            if (nextIdx > 1):
                nextIdx -= 1
                prevLogIdx = len(self.log)
                prevLogTerm = self.log[nextIdx - 1].term
            entries: AppendEntriesMembershipBody = AppendEntriesMembershipBody(self.currentTerm, self.address, prevLogIdx, 
                                                                    prevLogTerm, self.log, nextIdx, cluster_addr_new_list)
            print("Sending Cold,new log to all Cnew nodes...")
            tasks = []
            for cluster_addr in cluster_addr_new_list_notack:
                request: AppendEntriesMembershipRequest = AppendEntriesMembershipRequest(
                        cluster_addr, "receiver_replicate_log_coldnew_conf", entries)
                task = asyncio.ensure_future(self.__send_request_async(request))
                tasks.append(task)
            responses: List[AppendEntriesResponse] = await asyncio.gather(*tasks)
            
            cluster_addr_new_list_notack_new = []
            count_success = 0
            for i in range(len(responses)):
                response = responses[i]
                if response == None or not response.success:
                    cluster_addr_new_list_notack_new.append(cluster_addr_new_list_notack[i])
                else:
                    count_success += 1
            cluster_addr_new_list_notack = cluster_addr_new_list_notack_new
            if count_success >= len(cluster_addr_new_list) // 2: break

        print("Cold,new log replication to Cnew success...")
        print("Committing log...")
        self.log[len(self.log)-1].result = "Committed"
        self.update_commit_log()
        self.commitIdx += 1
        print("Cold,new log committed")
        print("Leader log: ", self.log, "\n")
    
        print("Sending response to client...")

        cluster_addr_list_all = self.cluster_addr_list.copy()
        cluster_addr_list_all.extend(cluster_addr_new_list)
        entries: AppendEntriesMembershipBody = AppendEntriesMembershipBody(self.currentTerm, self.address, None, None, self.log, self.commitIdx, [])
        tasks = []
        for cluster_addr in cluster_addr_list_all:
            request: AppendEntriesMembershipRequest = AppendEntriesMembershipRequest(
                    cluster_addr, "receiver_commit_log", entries)
            task = asyncio.ensure_future(self.__send_request_async(request))
            tasks.append(task)
        await asyncio.gather(*tasks)
        # Cold,new log committed on Cnew
        
    
    async def __cnew_log_sync(self, cluster_addr_new_list: list):
        # Try to replicate Cnew log on C_new
        log_entry = LogEntry(self.currentTerm, False, str(self.address), 
                             "Cnew", 1, None)
        self.log.append(log_entry)
        nextIdx = len(self.log)
        self.lastApplied += 1
        
        len_cluster_addr_list_old = len(self.cluster_addr_list)
        cluster_addr_list_notack = self.cluster_addr_list.copy()
        cluster_addr_new_list_notack = cluster_addr_new_list.copy()
       
        count_success = 0
        while True:
            nextIdx = 0
            prevLogIdx = -1
            prevLogTerm = -1

            if (nextIdx > 1):
                nextIdx -= 1
                prevLogIdx = len(self.log)
                prevLogTerm = self.log[nextIdx - 1].term

            entries = AppendEntriesMembershipBody(self.currentTerm, self.address, prevLogIdx, 
                                                                    prevLogTerm, self.log, nextIdx, cluster_addr_new_list)
            print("Sending Cold,new log to all Cnew nodes...")

            count_success = 0
            cluster_addr_list_notack_new = []
            tasks = []
            for cluster_addr in cluster_addr_list_notack:
                request: AppendEntriesMembershipRequest = AppendEntriesMembershipRequest(
                        cluster_addr, "receiver_replicate_log_cnew_conf", entries)
                task = asyncio.ensure_future(self.__send_request_async(request))
                tasks.append(task)
            responses: List[AppendEntriesResponse] = await asyncio.gather(*tasks)
            for i in range(len(responses)):
                response = responses[i]
                if response == None or not response.success:
                    cluster_addr_list_notack_new.append(cluster_addr_list_notack[i])
                else:
                    count_success += 1
            
            cluster_addr_new_list_notack_new = []
            for cluster_addr in cluster_addr_new_list_notack:
                request: AppendEntriesMembershipRequest = AppendEntriesMembershipRequest(
                        cluster_addr, "receiver_replicate_log_cnew_conf", entries)
                response: AppendEntriesResponse = await self.__send_request_async(request)
                if response == None or not response.success:
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
        self.update_commit_log()
        self.commitIdx += 1
        print("Cnew log committed")
        print("Leader log: ", self.log, "\n")
 
        entries: AppendEntriesMembershipBody = AppendEntriesMembershipBody(self.currentTerm, self.address, None, None, self.log, self.commitIdx, [])
        tasks = []
        for cluster_addr in self.cluster_addr_list:
            request: AppendEntriesMembershipRequest = AppendEntriesMembershipRequest(
                    cluster_addr, "receiver_commit_log", entries)
            task = asyncio.ensure_future(self.__send_request_async(request))
            tasks.append(task)
        await asyncio.gather(*tasks)
        
        # Cnew log committed on Cnew
        
    async def __joint_consensus(self):
        print("Start Joint Consensus")
        await asyncio.sleep(5)
        print("Joint Consensus Running...")
        print("Cold:", self.cluster_addr_list)
        print("Cnew:", self.cluster_addr_new_list)
        
        cluster_addr_new_list = self.cluster_addr_new_list.copy()
        self.cluster_addr_new_list = []
        
        await self.__coldnew_log_sync(cluster_addr_new_list)
        await self.__cnew_log_sync(cluster_addr_new_list)
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
            thr = Thread(target=asyncio.run, daemon=True, args=[self.__joint_consensus()])
            thr.start()
            
        response = MembershipResponse("success", self.address, self.log, self.cluster_addr_list)
        print(response)
        return json.dumps(response, cls=ResponseEncoder)

    def receiver_replicate_log_coldnew_conf(self, json_request: str):
        try:
            response_json_dumps = self.receiver_replicate_log(json_request)
            response: AppendEntriesResponse = json.loads(response_json_dumps, cls=ResponseDecoder)
            if not response.success:
                return response_json_dumps
            
            request: AppendEntriesMembershipRequest = json.loads(json_request, cls=RequestDecoder)
            bodyReq: AppendEntriesMembershipBody = request.body
            
            self.cluster_addr_new_list = bodyReq.cluster_addr_list
        
            print("Success to save Cold,new configuration...")
            print("Follower old configuration:", self.cluster_addr_list)
            print("Follower new configuration: ", self.cluster_addr_new_list)
            return response_json_dumps
        except Exception as e:
            print(e)
            response = AppendEntriesResponse(self.currentTerm, False)
            print(response)
            return json.dumps(response, cls=ResponseEncoder)
    
    def receiver_replicate_log_cnew_conf(self, json_request: str):
        self.cdTimer.reset()
        try:
            response_json_dumps = self.receiver_replicate_log(json_request)
            response: AppendEntriesResponse = json.loads(response_json_dumps, cls=ResponseDecoder)
            print(response)
            if not response.success:
                return response_json_dumps
            request: AppendEntriesMembershipRequest = json.loads(json_request, cls=RequestDecoder)
            bodyReq: AppendEntriesMembershipBody = request.body
    
            self.cluster_addr_list.extend(bodyReq.cluster_addr_list)
            self.cluster_addr_new_list = []
            
            print("Success to save Cnew configuration...")
            print("Follower configuration: ", self.cluster_addr_list)
            
            return response_json_dumps
        except Exception as e:
            print(e)
            response = AppendEntriesResponse(self.currentTerm, False)
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
    async def execute(self, json_request: str) -> str:
        request: ClientRequest = json.loads(json_request, cls=RequestDecoder)
        print("Request from Client\n", request, "\n")

        # Check leader or follower
        if self.cluster_leader_addr is not None:
            if self.address == self.cluster_leader_addr:
                response = ClientRequestResponse(request.body.requestNumber, "success", "result")
                print("Response to Client", response, "\n")
                log_entry = LogEntry(self.currentTerm, True, request.body.clientID, request.body.command, request.body.requestNumber, None)
                self.log.append(log_entry)
                await self.log_replication()
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

    async def log_replication(self):
        print("Log Replication")

        # Check whether commit index can be appended.
        if len(self.log)>0:
            newCommitIdx = 0
            for idx in range(len(self.log)-1, self.commitIdx, -1):
                count = len(dict(filter(lambda val: val[1]-1>= idx, self.nextIdx.items())))
                if count >= (len(self.cluster_addr_list)+1)//2 + 1:
                    newCommitIdx = idx
                    break
            for idx in range(self.commitIdx, newCommitIdx+1):
                if self.log[idx].operation.startswith('enqueue') or self.log[idx].operation.startswith('dequeue'):
                    self.commit_entry(idx)
                    self.lastApplied +=1
            self.commitIdx = newCommitIdx
        
        # Send append entries that append AND commit logs. Remember that not all server will return (caused of network problem), hence the code above is possible
        tasks = []
        for addr in self.cluster_addr_list:
            request = AppendEntriesRequest(addr, "receiver_log_replication", AppendEntriesBody(self.currentTerm, self.address, self.nextIdx[addr]-1, 0 if len(self.log)<=1 else self.log[self.nextIdx[addr]-1].term, self.log[self.nextIdx[addr]:], self.commitIdx))
            task = asyncio.ensure_future(self.__send_request_async(request))
            tasks.append(task)
        responses: List[AppendEntriesResponse | None] = await asyncio.gather(*tasks)
        for res in responses:
            if res:
                if res.term <= self.currentTerm:
                    self.currentTerm=res.term
                    if res.dest:
                        if res.success:
                            self.nextIdx[res.dest] = len(self.log)
                            self.matchIdx[res.dest] = self.commitIdx
                        else:
                            self.nextIdx[res.dest] = 0
                            self.matchIdx[res.dest] = 0
                else:
                    self.type = RaftNode.NodeType.FOLLOWER

    async def receiver_log_replication(self, json_request: str) -> str:
        print("Receiver replicate log...")
        self.cdTimer.reset()
        request: AppendEntriesRequest = json.loads(json_request, cls=RequestDecoder)
        response: AppendEntriesResponse = AppendEntriesResponse(self.currentTerm, False)

        if(request.body.term < self.currentTerm):
            # Term is greater than leader, this indicates this node is more updated
            return json.dumps(response, cls=ResponseEncoder)
        
        if(self.log[request.body.prevLogIdx].term != request.body.term):
            # Incorrect previous log, reset log and MQ
            self.log = []
            self.app.clear()
            return json.dumps(response, cls=ResponseEncoder)
        
        if(request.body.prevLogIdx < len(self.log)-1):
            # Delete unused log (different tail than leader)
            for idx in range(len(self.log)-1, request.body.prevLogIdx):
                self.undo_entry(idx)
            del self.log[request.body.prevLogIdx+1:]
        
        # Append entries
        self.log += request.body.entries
        response.success = True

        # Commit until leader's commitIdx
        for idx in range(self.commitIdx, request.body.leaderCommit+1):
            self.commit_entry(idx)
            self.lastApplied +=1
        self.commitIdx = request.body.leaderCommit

        return json.dumps(response, cls=ResponseEncoder)

    def commit_entry(self, idx: int):
        # TODO Check duplicate and operation type
        # Kalo sukses panggil bawah ini
        self.app.apply(self.log[idx].operation)
    
    def undo_entry(self, idx: int):
        # TODO Cek undo dan duplicate
        pass
import sys
from typing import Optional
from xmlrpc.server import SimpleXMLRPCServer

from lib.app import MessageQueue
from lib.raft import RaftNode
from lib.struct.address import Address


def start_serving(addr: Address, contact_node_addr: Optional[Address]):
    print(f"Starting Raft Server at {addr.ip}:{addr.port}")
    with SimpleXMLRPCServer((addr.ip, addr.port)) as server:
        server.register_introspection_functions()
        server.register_instance(RaftNode(MessageQueue(), addr, contact_node_addr))
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nKeyboard interrupt received, exiting.")
            server.shutdown()
            sys.exit(0)
        

if __name__ == "__main__":
    if len(sys.argv) < 3:
        # contact node is leader node
        print(sys.argv)
        print("server.py <ip> <port> <opt: contact ip> <opt: contact port>")
        exit()

    contact_addr = None
    if len(sys.argv) == 5:
        contact_addr = Address(sys.argv[3], int(sys.argv[4]))
    server_addr = Address(sys.argv[1], int(sys.argv[2]))

    start_serving(server_addr, contact_addr)
    
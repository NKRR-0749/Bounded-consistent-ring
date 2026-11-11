#!/usr/bin/env python3
"""
network_node.py - Standalone Network Router & Bridge for Multi-Laptop BoundedRing
---------------------------------------------------------------------------------
Enables distributed consistent hash message routing across 4 physical laptops
via standard TCP sockets and JSON serialization without external dependencies.
"""

import sys
import os
import json
import socket
import threading
import argparse
import hashlib
from typing import Dict, Any, Optional

# Default Network Topology for the 4 physical laptops
DEFAULT_NODES = {
    "Server_Alpha": {"host": "172.29.7.133", "port": 5000},
    "Server_Beta":  {"host": "172.29.7.133", "port": 5001},
    "Server_Gamma": {"host": "172.29.7.133", "port": 5002},
    "Server_Delta": {"host": "172.29.7.133", "port": 5003},
}

# The ordered circular ring progression for clockwise skips
RING_ORDER = ["Server_Alpha", "Server_Beta", "Server_Gamma", "Server_Delta"]

CAPACITY_PER_NODE = 3


def compute_sha256(payload: str) -> str:
    """Compute 16-character hex checksum matching the C++ hash verification."""
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]


class NetworkNode:
    def __init__(self, node_id: str, config: Dict[str, Dict[str, Any]], capacity: int = CAPACITY_PER_NODE, bind_all: bool = True):
        if node_id not in config:
            raise ValueError(f"Unknown node '{node_id}'. Configured nodes are: {list(config.keys())}")

        self.node_id = node_id
        self.config = config
        self.capacity = capacity
        
        # When running on a machine, binding to 0.0.0.0 listens on all local network adapters
        self.bind_host = "0.0.0.0" if bind_all else config[node_id]["host"]
        self.port = config[node_id]["port"]
        
        self.storage: Dict[str, Dict[str, str]] = {}
        self.lock = threading.Lock()
        self.running = False
        self.server_socket: Optional[socket.socket] = None

    def get_load(self) -> int:
        with self.lock:
            return len(self.storage)

    def is_full(self) -> bool:
        with self.lock:
            return len(self.storage) >= self.capacity

    def get_next_node_id(self, current_id: str) -> str:
        idx = RING_ORDER.index(current_id)
        return RING_ORDER[(idx + 1) % len(RING_ORDER)]

    def handle_client(self, conn: socket.socket, addr: tuple):
        """Handle incoming connection from another node or message publisher."""
        try:
            # Read complete JSON message packet
            buffer = ""
            while True:
                chunk = conn.recv(4096).decode('utf-8')
                if not chunk:
                    break
                buffer += chunk
                if "\n" in buffer:  # End of message delimiter
                    break

            if not buffer.strip():
                return

            try:
                req = json.loads(buffer.strip())
            except json.JSONDecodeError as e:
                print(f"[{self.node_id}] Received malformed JSON: {e}")
                return

            action = req.get("action", "route")
            action = req.get("action", "route")

            if action == "route":
                msg_key = req.get("key", "")
                payload = req.get("payload", "")
                hop_count = req.get("hop_count", 0)
                origin_node = req.get("origin_node", self.node_id)
                visited = req.get("visited", [])

                response = self.process_or_forward_message(msg_key, payload, hop_count, origin_node, visited)
                conn.sendall((json.dumps(response) + "\n").encode('utf-8'))

            elif action == "status":
                with self.lock:
                    status_info = {
                        "node_id": self.node_id,
                        "load": len(self.storage),
                        "capacity": self.capacity,
                        "is_full": len(self.storage) >= self.capacity,
                        "stored_keys": list(self.storage.keys())
                    }
                conn.sendall((json.dumps(status_info) + "\n").encode('utf-8'))

            elif action == "clear":
                with self.lock:
                    self.storage.clear()
                conn.sendall((json.dumps({"status": "cleared", "node_id": self.node_id}) + "\n").encode('utf-8'))

        except Exception as e:
            print(f"[{self.node_id}] Error handling request from {addr}: {e}")
        finally:
            conn.close()

    def process_or_forward_message(self, key: str, payload: str, hop_count: int, origin_node: str, visited: list) -> Dict[str, Any]:
        """
        Clockwise Bounded Probe Logic:
        1. If local capacity has space (< 3): Store locally.
        2. If local capacity == 3: Forward to next node in circular ring.
        3. If all 4 nodes have been visited and are full: Return OVERFLOW.
        """
        visited = visited + [self.node_id]

        with self.lock:
            # Can we accept this message locally?
            if len(self.storage) < self.capacity:
                checksum = compute_sha256(payload)
                self.storage[key] = {
                    "key": key,
                    "payload": payload,
                    "checksum": checksum
                }
                current_load = len(self.storage)

                if hop_count == 0:
                    status_tag = f"[DIRECT HIT] Stored locally on {self.node_id}"
                else:
                    status_tag = f"[SKIPPED {hop_count} FULL NODE(S)] -> Stored on {self.node_id}"

                print(f"[{self.node_id}] {status_tag} (Load: {current_load}/{self.capacity}) | Key: {key}")

                return {
                    "status": "SUCCESS",
                    "routed_to": self.node_id,
                    "hop_count": hop_count,
                    "visited": visited,
                    "load": current_load,
                    "capacity": self.capacity,
                    "checksum": checksum
                }

        # Local node is full (3/3), check if we have probed all 4 laptops
        if len(visited) >= len(RING_ORDER):
            print(f"[{self.node_id}] └─ [OVERFLOW] All {len(RING_ORDER)} nodes are at maximum capacity! Cannot route key: {key}")
            return {
                "status": "OVERFLOW",
                "routed_to": None,
                "hop_count": hop_count + 1,
                "visited": visited,
                "message": "All servers are full"
            }

        # Otherwise, forward clockwise to next node
        next_node = self.get_next_node_id(self.node_id)
        print(f"[{self.node_id}] Local node FULL ({self.capacity}/{self.capacity}). Forwarding -> {next_node} (Hop: {hop_count + 1})")
        
        forward_result = self.send_to_remote_node(
            target_node_id=next_node,
            payload_data={
                "action": "route",
                "key": key,
                "payload": payload,
                "hop_count": hop_count + 1,
                "origin_node": origin_node,
                "visited": visited
            }
        )
        return forward_result

    def send_to_remote_node(self, target_node_id: str, payload_data: Dict[str, Any]) -> Dict[str, Any]:
        """Establish TCP socket client connection to forward packet to another laptop."""
        target_info = self.config.get(target_node_id)
        if not target_info:
            return {"status": "ERROR", "message": f"Unknown target {target_node_id}"}

        host = target_info["host"]
        port = target_info["port"]

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5.0)  # 5 second timeout for remote laptop response
            s.connect((host, port))
            s.sendall((json.dumps(payload_data) + "\n").encode('utf-8'))

            # Await reply
            buffer = ""
            while True:
                chunk = s.recv(4096).decode('utf-8')
                if not chunk:
                    break
                buffer += chunk
                if "\n" in buffer:
                    break

            s.close()
            if buffer.strip():
                return json.loads(buffer.strip())
            return {"status": "ERROR", "message": "Empty response from node"}
        except (socket.timeout, ConnectionRefusedError, OSError) as e:
            print(f"[{self.node_id}] Failed connecting to {target_node_id} at {host}:{port} ({e})")
            return {
                "status": "CONNECTION_FAILED",
                "target": target_node_id,
                "host": host,
                "port": port,
                "error": str(e)
            }

    def start(self):
        """Start listening on assigned port."""
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.bind_host, self.port))
        self.server_socket.listen(10)
        self.server_socket.settimeout(1.0)  # Allows clean Ctrl+C interruption

        print("=" * 60)
        print(f"  BOUNDEDRING DISTRIBUTED NODE : {self.node_id}")
        print("=" * 60)
        print(f"  [+] Bound on address  : {self.bind_host}:{self.port}")
        print(f"  [+] Configured IP     : {self.config[self.node_id]['host']}")
        print(f"  [+] Node Capacity     : {self.capacity} messages")
        print(f"  [+] Clockwise Neighbor: {self.get_next_node_id(self.node_id)}")
        print("=" * 60)
        print(f"[{self.node_id}] Listening for incoming peer messages...\n")

        try:
            while self.running:
                try:
                    conn, addr = self.server_socket.accept()
                    t = threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True)
                    t.start()
                except socket.timeout:
                    continue  # Loop back to check self.running / KeyboardInterrupt
        except KeyboardInterrupt:
            print(f"\n[{self.node_id}] Shutting down node gracefully...")
        finally:
            if self.server_socket:
                self.server_socket.close()

    def print_status(self):
        with self.lock:
            print(f"\n[{self.node_id}] Current Messages ({len(self.storage)}/{self.capacity}):")
            for k, v in self.storage.items():
                print(f"  - Key: {k} | SHA256: {v['checksum']} | Payload: {v['payload']}")


# CLI Message Sender Helper
def send_message_cli(target_node_id: str, key: str, payload: str, config: Dict[str, Dict[str, Any]]):
    target = config.get(target_node_id)
    if not target:
        print(f"Error: Unknown node '{target_node_id}'")
        return

    req = {
        "action": "route",
        "key": key,
        "payload": payload,
        "hop_count": 0,
        "origin_node": target_node_id,
        "visited": []
    }
    
    print(f"[CLI] Sending message '{key}' directly to {target_node_id} ({target['host']}:{target['port']})...")
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        s.connect((target["host"], target["port"]))
        s.sendall((json.dumps(req) + "\n").encode('utf-8'))
        resp = s.recv(4096).decode('utf-8')
        s.close()
        print(f"[CLI Result]: {resp.strip()}")
    except Exception as e:
        print(f"[CLI Error]: Could not send to {target_node_id}: {e}")


def main():
    parser = argparse.ArgumentParser(description="BoundedRing Multi-Laptop Network Bridge")
    parser.add_argument("--node", type=str, required=True,
                        help="Identity of this node (Server_Alpha, Server_Beta, Server_Gamma, Server_Delta)")
    parser.add_argument("--config", type=str, default="nodes_config.json",
                        help="Path to network topology JSON file (default: nodes_config.json)")
    parser.add_argument("--capacity", type=int, default=CAPACITY_PER_NODE,
                        help=f"Message storage capacity (default: {CAPACITY_PER_NODE})")
    parser.add_argument("--send-key", type=str, default=None,
                        help="Send a test message with this key to --node instead of running server")
    parser.add_argument("--send-payload", type=str, default="Test_Payload",
                        help="Payload content when using --send-key")

    args = parser.parse_args()

    # Load configuration from JSON file if present, otherwise fall back to DEFAULT_NODES
    config = DEFAULT_NODES.copy()
    if os.path.exists(args.config):
        try:
            with open(args.config, "r") as f:
                loaded_config = json.load(f)
                config.update(loaded_config)
            print(f"[*] Loaded network config from '{args.config}'")
        except Exception as e:
            print(f"[!] Warning: Failed to load '{args.config}': {e}. Using default topology.")

    # Sender mode execution
    if args.send_key:
        send_message_cli(args.node, args.send_key, args.send_payload, config)
        return

    # Server mode execution
    node = NetworkNode(node_id=args.node, config=config, capacity=args.capacity)
    node.start()


if __name__ == "__main__":
    main()


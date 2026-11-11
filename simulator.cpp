#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <cstdint>

uint64_t hash_fn(const std::string& str) {
    uint64_t hash = 14695981039346656037ULL;
    for (char c : str) {
        hash ^= static_cast<uint64_t>(c);
        hash *= 1099511628211ULL;
    }
    return hash;
}

std::string compute_sha256(const std::string& msg) {
    std::stringstream ss;
    uint64_t h = hash_fn("SHA256_" + msg);
    ss << std::hex << std::setfill('0') << std::setw(16) << h;
    return ss.str();
}

struct Message {
    std::string key;
    std::string payload;
    std::string checksum;
};

struct VirtualNode {
    uint64_t hash_val;
    std::string server_id;
};

class ServerNode {
public:
    std::string server_id;
    int capacity;
    std::unordered_map<std::string, Message> storage;

    ServerNode() : server_id(""), capacity(0) {}
    ServerNode(std::string id, int cap) : server_id(id), capacity(cap) {}

    bool is_full() const {
        return storage.size() >= static_cast<size_t>(capacity);
    }

    bool store_message(const Message& msg) {
        if (is_full()) return false;
        storage[msg.key] = msg;
        return true;
    }

    int get_load() const {
        return storage.size();
    }
};

class BoundedRingRouter {
private:
    int vnodes_per_server_;
    int default_capacity_;
    std::vector<VirtualNode> ring_;
    std::unordered_map<std::string, ServerNode> servers_;

    void rebuild_ring() {
        ring_.clear();
        for (const auto& pair : servers_) {
            const std::string& server_id = pair.first;
            for (int i = 0; i < vnodes_per_server_; ++i) {
                std::string vnode_key = server_id + "#vnode_" + std::to_string(i);
                ring_.push_back({hash_fn(vnode_key), server_id});
            }
        }
        std::sort(ring_.begin(), ring_.end(), [](const VirtualNode& a, const VirtualNode& b) {
            return a.hash_val < b.hash_val;
        });
    }

public:
    BoundedRingRouter(int vnodes_per_server, int default_capacity) 
        : vnodes_per_server_(vnodes_per_server), default_capacity_(default_capacity) {}

    void add_server(const std::string& server_id) {
        servers_[server_id] = ServerNode(server_id, default_capacity_);
        rebuild_ring();
    }

    std::string route_message(const std::string& msg_key, const std::string& payload) {
        if (ring_.empty()) return "NO_SERVERS";

        uint64_t msg_hash = hash_fn(msg_key);

        auto it = std::lower_bound(ring_.begin(), ring_.end(), msg_hash,
            [](const VirtualNode& vn, uint64_t val) {
                return vn.hash_val < val;
            });

        size_t start_idx = (it == ring_.end()) ? 0 : std::distance(ring_.begin(), it);
        size_t curr_idx = start_idx;
        int probes = 0;

        while (true) {
            std::string target_server_id = ring_[curr_idx].server_id;
            ServerNode& target = servers_[target_server_id];

            if (!target.is_full()) {
                Message msg = {msg_key, payload, compute_sha256(payload)};
                target.store_message(msg);

                if (probes > 0) {
                    std::cout << "  └─ [SKIPPED " << probes << " FULL NODE(S)] -> Routed to " 
                              << target_server_id << " (Load: " << target.get_load() << "/" << target.capacity << ")\n";
                } else {
                    std::cout << "  └─ [DIRECT HIT] Routed to " << target_server_id 
                              << " (Load: " << target.get_load() << "/" << target.capacity << ")\n";
                }
                return target_server_id;
            }

            probes++;
            curr_idx = (curr_idx + 1) % ring_.size();
            if (curr_idx == start_idx) break;
        }

        std::cout << "  └─ [OVERFLOW] All servers are at maximum capacity!\n";
        return "OVERFLOW";
    }

    void print_server_status() {
        std::cout << "\n=========================================\n";
        std::cout << "         4-SYSTEM SERVER METRICS         \n";
        std::cout << "=========================================\n";
        for (const auto& pair : servers_) {
            const ServerNode& node = pair.second;
            std::cout << "  " << node.server_id << ": " << node.get_load() << " / " << node.capacity 
                      << " messages stored";
            if (node.is_full()) std::cout << " [FULL]";
            std::cout << "\n";
        }
        std::cout << "=========================================\n\n";
    }
};

int main() {
    BoundedRingRouter router(3, 3);

    std::cout << "=========================================\n";
    std::cout << "  BOUNDEDRING 4-SYSTEM SIMULATOR DEMO    \n";
    std::cout << "=========================================\n\n";

    std::cout << "[+] Booting 4 Independent Server Nodes...\n";
    router.add_server("Server_Alpha");
    router.add_server("Server_Beta");
    router.add_server("Server_Gamma");
    router.add_server("Server_Delta");

    router.print_server_status();

    std::cout << "[>] Simulating Message Ingestion Stream (14 Messages):\n\n";
    for (int i = 1; i <= 14; ++i) {
        std::string key = "msg_key_" + std::to_string(i * 37);
        std::string payload = "Data_Payload_" + std::to_string(i);

        std::cout << "Message #" << i << " ('" << key << "'):\n";
        router.route_message(key, payload);
    }

    router.print_server_status();

    return 0;
}
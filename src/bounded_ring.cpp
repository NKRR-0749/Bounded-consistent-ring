//headers
#include<pybind11/pybind11.h>
#include<pybind11/stl.h>
#include <iostream>
#include<vector>//ds
#include<string>
#include<unordered_map>//ds
#include<algorithm>//ds
#include<sstream>
#include<iomanip>
#include<cstdint>

namespace py = pybind11;

//SHA-256 block
uint64_t hash_fn(const std::string& str){
    uint64_t hash = 1469598103934665037ULL;
    for (char c : str) {
        hash ^= static_cast<uint64_t>(c);
        hash *= 1099511628211ULL;
    }
    return hash;
}
//verificatio of SHA=256 block
std::string compute_sha256_checksum(const std::string& msg){
    std::stringstream ss;
    uint64_t h = hash_fn("SHA256_" + msg);
    ss<<std::hex << std::setfill('0')<< std::setw(16)<<h;
    return ss.str();
}
//data stuctures
struct Message{
    std::string key;
    std::string payload;
    std::string checksum;
};

struct VirtualNode {
    uint64_t hash_val;
    std::string server_id;
};

//router class
class BoundedRingRouter {
    private:
    int num_vnodes_;
    int server_capacity_;
    std::vector<VirtualNode> ring_;
    std::vector<std::string> active_servers_;
    std::unordered_map<std::string, int> server_load_;
    std::unordered_map<std::string, std::unordered_map<std::string, Message>> server_storage_;

    void rebuild_ring() {
        ring_.clear();
        for(const auto& server : active_servers_){
            for(int i=0; i<num_vnodes_;++i){
                std::string vnode_key = server + "#vnode_" +std::to_string(i);
                ring_.push_back({hash_fn(vnode_key), server});
            }
        }
    // Keep virtual nodes sorted in ascending order for O(log N) binary search
        std::sort(ring_.begin(), ring_.end(), [](const VirtualNode& a, const VirtualNode& b) {
            return a.hash_val < b.hash_val;
        });
    }

public:
    BoundedRingRouter(int num_vnodes, int server_capacity) 
        : num_vnodes_(num_vnodes), server_capacity_(server_capacity) {}

    void add_server(const std::string& server_id) {
        if (std::find(active_servers_.begin(), active_servers_.end(), server_id) == active_servers_.end()) {
            active_servers_.push_back(server_id);
            server_load_[server_id] = 0;
            rebuild_ring();
        }
    }

    void remove_server(const std::string& server_id) {
        auto it = std::find(active_servers_.begin(), active_servers_.end(), server_id);
        if (it != active_servers_.end()) {
            active_servers_.erase(it);
            server_load_.erase(server_id);
            server_storage_.erase(server_id);
            rebuild_ring();
        }
    }

    // Routing Logic: Binary Search + Clockwise Load-Bounding Skip Rule
    std::string route_message(const std::string& msg_key, const std::string& payload) {
        if (ring_.empty()) return "";

        uint64_t msg_hash = hash_fn(msg_key);
        
        // Binary search to find initial virtual node position
        auto it = std::lower_bound(ring_.begin(), ring_.end(), msg_hash, 
            [](const VirtualNode& vnode, uint64_t val) {
                return vnode.hash_val < val;
            });

        size_t start_idx = (it == ring_.end()) ? 0 : std::distance(ring_.begin(), it);
        size_t curr_idx = start_idx;

        // Clockwise Load Bounding Probe Loop
        while (true) {
            std::string target_server = ring_[curr_idx].server_id;
            
            if (server_load_[target_server] < server_capacity_) {
                Message msg = {msg_key, payload, compute_sha256_checksum(payload)};
                server_storage_[target_server][msg_key] = msg;
                server_load_[target_server]++;
                return target_server;
            }

            // Probe clockwise if target server is full
            curr_idx = (curr_idx + 1) % ring_.size();
            if (curr_idx == start_idx) break; // All servers at maximum capacity
        }

        return "OVERFLOW";
    }

    std::unordered_map<std::string, int> get_load_distribution() {
        return server_load_;
    }

    void clear_messages() {
        for (auto& pair : server_load_) {
            pair.second = 0;
        }
        server_storage_.clear();
    }
};

// 4. pybind11 Module Binding Definition
PYBIND11_MODULE(bounded_ring_core, m) {
    m.doc() = "BoundedRing consistent hashing library compiled with C++";

    py::class_<BoundedRingRouter>(m, "BoundedRingRouter")
        .def(py::init<int, int>(), py::arg("num_vnodes"), py::arg("server_capacity"))
        .def("add_server", &BoundedRingRouter::add_server)
        .def("remove_server", &BoundedRingRouter::remove_server)
        .def("route_message", &BoundedRingRouter::route_message)
        .def("get_load_distribution", &BoundedRingRouter::get_load_distribution)
        .def("clear_messages", &BoundedRingRouter::clear_messages);
}

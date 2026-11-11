# BoundedRing: Distributed Consistent Hashing with Load-Bounding & Cryptographic Verification

A C++ and Python distributed consistent hashing system implementing the **Clockwise Load-Bounding Skip Rule** and **SHA-256 Checksum Verification** across a 4-node cluster with interactive web visualization.

---

## 🌟 Features
- **Bounded Load Skip Rule**: Nodes enforce a maximum capacity bound (e.g. 3 messages). Full nodes are probed clockwise until an available node is found.
- **Cryptographic Verification**: Every payload generates a 16-character SHA-256 style signature for payload integrity.
- **Multi-Laptop Network Bridge (`network_node.py`)**: Zero-dependency TCP socket server to run across 4 physical machines over Wi-Fi/LAN.
- **Interactive Web Visualizer (`web/index.html`)**: Real-time Cisco-style packet trajectory tracer, 360° virtual node ring, and cryptographic inspection pipeline.
- **C++ Core Engine & Standalone CLI Simulator (`simulator.cpp`, `src/bounded_ring.cpp`)**.

---

## 📁 Repository Structure
```
├── src/
│   └── bounded_ring.cpp     # C++ pybind11 core module
├── web/
│   └── index.html           # Interactive visualizer & packet tracer
├── simulator.cpp            # Pure C++ standalone CLI simulation
├── network_node.py          # Distributed TCP socket bridge for 4 laptops
├── nodes_config.json        # 4-node cluster IP & port configuration
├── run_simulation.py        # Python benchmark & matplotlib visualization
├── CMakeLists.txt           # CMake build configuration
└── .gitignore               # Git ignore rules for build artifacts & binaries
```

---

## 🚀 Getting Started

### 1. Standalone C++ Simulator
```bash
g++ -std=c++17 simulator.cpp -o simulator.exe
./simulator.exe
```

### 2. Interactive Web Visualizer
Open `web/index.html` directly in any modern browser.

### 3. Distributed 4-Laptop Network
Run on each laptop:
```bash
python network_node.py --node Server_Alpha   # Laptop 1
python network_node.py --node Server_Beta    # Laptop 2
python network_node.py --node Server_Gamma   # Laptop 3
python network_node.py --node Server_Delta   # Laptop 4
```

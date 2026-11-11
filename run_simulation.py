import os
import sys

# Tell Windows Python where to find GCC runtime DLLs
mingw_bin = r"C:\Users\Jyotsna\AppData\Local\Microsoft\WinGet\Packages\BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe\mingw64\bin"
if os.path.exists(mingw_bin):
    os.add_dll_directory(mingw_bin)

import bounded_ring_core
import matplotlib.pyplot as plt
import random
import string
import bounded_ring_core
import matplotlib.pyplot as plt
import random
import string

def generate_key(length=12):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def run_experiment():
    num_servers = 10
    vnodes_per_server = 50
    capacity_per_server = 120
    total_messages = 1000

    # Instantiate C++ engine
    router = bounded_ring_core.BoundedRingRouter(vnodes_per_server, capacity_per_server)

    for i in range(num_servers):
        router.add_server(f"Server_{i}")

    for i in range(total_messages):
        router.route_message(generate_key(), f"Payload_{i}")

    # Fetch results from C++
    load_dist = router.get_load_distribution()

    # Plot Load Distribution
    plt.figure(figsize=(10, 5))
    plt.bar(load_dist.keys(), load_dist.values(), color='skyblue', edgecolor='black')
    plt.axhline(y=capacity_per_server, color='r', linestyle='--', label=f'Capacity Bound ({capacity_per_server})')
    plt.title('BoundedRing Load Spread Performance')
    plt.xlabel('Server ID')
    plt.ylabel('Message Count')
    plt.legend()
    plt.tight_layout()
    plt.savefig('load_distribution.png')
    plt.show()

if __name__ == '__main__':
    run_experiment()
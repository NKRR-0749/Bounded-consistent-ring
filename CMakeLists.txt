cmake_minimum_required(VERSION 3.12)
project(bounded_ring_core)

set(CMAKE_CXX_STANDARD 17)

# 1. Find both Python Interpreter AND Development libraries
find_package(Python 3.12 COMPONENTS Interpreter Development REQUIRED)

# 2. Add pybind11 directory to search path
set(pybind11_DIR "C:/Users/Jyotsna/AppData/Local/Programs/Python/Python312/Lib/site-packages/pybind11/share/cmake/pybind11")

# 3. Find pybind11 and construct module
find_package(pybind11 REQUIRED)
pybind11_add_module(bounded_ring_core src/bounded_ring.cpp)
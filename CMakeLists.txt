cmake_minimum_required(VERSION 3.20)
project(StrataDB LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(stratadb
    src/main.cpp 
    src/storage/disk_manager.cpp
    src/index/node.cpp
)

target_include_directories(stratadb PRIVATE ${PROJECT_SOURCE_DIR}/include)
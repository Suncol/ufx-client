cmake_minimum_required(VERSION 3.5)
project(ufx_client CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

add_library(ufx_core
    src/books.cpp
    src/coalescer.cpp)
target_include_directories(ufx_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(test_books tests/test_books.cpp)
target_link_libraries(test_books PRIVATE ufx_core)

option(UFX_WITH_T2SDK "Build the T2SDK session and demo" OFF)
set(T2SDK_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../T2SDK_第三方版本/c++" CACHE PATH "T2SDK root")

if(UFX_WITH_T2SDK)
    add_library(ufx_session src/session.cpp)
    target_include_directories(ufx_session
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src ${T2SDK_ROOT}/Include
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_link_libraries(ufx_session PUBLIC ufx_core)

    if(WIN32)
        if(CMAKE_SIZEOF_VOID_P EQUAL 8)
            target_link_libraries(ufx_session PRIVATE ${T2SDK_ROOT}/win64/lib/t2sdk.lib)
        else()
            target_link_libraries(ufx_session PRIVATE ${T2SDK_ROOT}/win32/lib/t2sdk.lib)
        endif()
    else()
        target_link_libraries(ufx_session PRIVATE ${T2SDK_ROOT}/linux.x64/lib/libt2sdk.so pthread)
    endif()

    add_executable(ufx_demo demo/main.cpp)
    target_include_directories(ufx_demo PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/demo)
    target_link_libraries(ufx_demo PRIVATE ufx_session)
endif()

cmake_minimum_required(VERSION 3.16)
project(HelloWorld CXX)

set(CONAN_SYSTEM_INCLUDES TRUE)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_DEFAULT 17)
set(CXX17 ON)
set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_C_EXTENSIONS ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdata-sections -ffunction-sections")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fdata-sections -ffunction-sections")
add_link_options("-Wl,--gc-sections")

include(${PROJECT_SOURCE_DIR}/cmake/conan.cmake)

conan_cmake_autodetect(CONAN_SETTINGS)

conan_cmake_run(CONANFILE conanfile.txt
        BASIC_SETUP CMAKE_TARGETS
        BUILD missing
        SETTINGS ${CONAN_SETTINGS}
        ENV "CXXFLAGS=-fdata-sections -ffunction-sections"
        ENV "CFLAGS=-fdata-sections -ffunction-sections")

set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS conanfile.txt)

if (EXISTS "${CMAKE_BINARY_DIR}/conanbuildinfo.cmake")
    include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
    conan_basic_setup()
    message(STATUS "CONAN_LIBS: ${CONAN_LIBS}")
    set(DEFAULT_LIBS ${CONAN_LIBS})
else ()
    message(FATAL_ERROR "The file conanbuildinfo.cmake doesn't exist, check if conan install was executed.")
endif ()

link_directories(SYSTEM ${CONAN_LIB_DIRS})
link_libraries(${CONAN_LIBS})

add_executable(HelloWorld src/main.cpp)

include(CTest)
enable_testing()

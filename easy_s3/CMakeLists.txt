cmake_minimum_required(VERSION 2.8)
project(easy_s3 C)

# This makefile expects LIBS3_INCLUDE_DIRS and LIBS3_LIBRARIES to be set

include_directories(${LIBS3_INCLUDE_DIRS})
add_definitions(-g)

add_library(easy_s3 STATIC easy_s3.c)
target_link_libraries(easy_s3 ${LIBS3_LIBRARIES})

add_executable(example example.c)
target_link_libraries(example easy_s3)


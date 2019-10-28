#cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DCMAKE_CXX_FLAGS="-O0 -g3 -gdwarf-4" -DCMAKE_C_FLAGS="-O0 -g3 -gdwarf-4" .

cmake_minimum_required (VERSION 3.11)

project (tbplus)

add_executable(tbplus
  tbplus.cxx
  bplus_leaf.cxx
#  ceph_crypto.cc
#  ${CMAKE_SOURCE_DIR}/blake2/sse/blake2b.c
#  ${CMAKE_SOURCE_DIR}/blake2/sse/blake2bp.c
  )

set_property(TARGET tbplus PROPERTY CXX_STANDARD 17)
target_link_libraries(tbplus)

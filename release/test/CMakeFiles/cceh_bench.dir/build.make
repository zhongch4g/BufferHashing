# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.16

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/local/bin/cmake

# The command to remove a file.
RM = /usr/local/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/zhongchen/CCEH-BUFLOG

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/zhongchen/CCEH-BUFLOG/release

# Include any dependencies generated for this target.
include test/CMakeFiles/cceh_bench.dir/depend.make

# Include the progress variables for this target.
include test/CMakeFiles/cceh_bench.dir/progress.make

# Include the compile flags for this target's objects.
include test/CMakeFiles/cceh_bench.dir/flags.make

test/CMakeFiles/cceh_bench.dir/cceh_bench.cpp.o: test/CMakeFiles/cceh_bench.dir/flags.make
test/CMakeFiles/cceh_bench.dir/cceh_bench.cpp.o: ../test/cceh_bench.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/zhongchen/CCEH-BUFLOG/release/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/CMakeFiles/cceh_bench.dir/cceh_bench.cpp.o"
	cd /home/zhongchen/CCEH-BUFLOG/release/test && /usr/bin/clang++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/cceh_bench.dir/cceh_bench.cpp.o -c /home/zhongchen/CCEH-BUFLOG/test/cceh_bench.cpp

test/CMakeFiles/cceh_bench.dir/cceh_bench.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/cceh_bench.dir/cceh_bench.cpp.i"
	cd /home/zhongchen/CCEH-BUFLOG/release/test && /usr/bin/clang++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/zhongchen/CCEH-BUFLOG/test/cceh_bench.cpp > CMakeFiles/cceh_bench.dir/cceh_bench.cpp.i

test/CMakeFiles/cceh_bench.dir/cceh_bench.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/cceh_bench.dir/cceh_bench.cpp.s"
	cd /home/zhongchen/CCEH-BUFLOG/release/test && /usr/bin/clang++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/zhongchen/CCEH-BUFLOG/test/cceh_bench.cpp -o CMakeFiles/cceh_bench.dir/cceh_bench.cpp.s

# Object files for target cceh_bench
cceh_bench_OBJECTS = \
"CMakeFiles/cceh_bench.dir/cceh_bench.cpp.o"

# External object files for target cceh_bench
cceh_bench_EXTERNAL_OBJECTS =

cceh_bench: test/CMakeFiles/cceh_bench.dir/cceh_bench.cpp.o
cceh_bench: test/CMakeFiles/cceh_bench.dir/build.make
cceh_bench: /usr/lib/x86_64-linux-gnu/libtbb.so
cceh_bench: CCEH/src/libcceh.a
cceh_bench: test/CMakeFiles/cceh_bench.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/zhongchen/CCEH-BUFLOG/release/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable ../cceh_bench"
	cd /home/zhongchen/CCEH-BUFLOG/release/test && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/cceh_bench.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
test/CMakeFiles/cceh_bench.dir/build: cceh_bench

.PHONY : test/CMakeFiles/cceh_bench.dir/build

test/CMakeFiles/cceh_bench.dir/clean:
	cd /home/zhongchen/CCEH-BUFLOG/release/test && $(CMAKE_COMMAND) -P CMakeFiles/cceh_bench.dir/cmake_clean.cmake
.PHONY : test/CMakeFiles/cceh_bench.dir/clean

test/CMakeFiles/cceh_bench.dir/depend:
	cd /home/zhongchen/CCEH-BUFLOG/release && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/zhongchen/CCEH-BUFLOG /home/zhongchen/CCEH-BUFLOG/test /home/zhongchen/CCEH-BUFLOG/release /home/zhongchen/CCEH-BUFLOG/release/test /home/zhongchen/CCEH-BUFLOG/release/test/CMakeFiles/cceh_bench.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/CMakeFiles/cceh_bench.dir/depend


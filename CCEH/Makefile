.PHONY: all clean

CXX := g++-10 -O3 -std=c++17 -g
CFLAGS := -I./ -lrt

all: ycsb

ycsb: src/ycsb_bench.cpp CCEH TRACE
	$(CXX) $(CFLAGS) -o ycsb_bench src/ycsb_bench.cpp src/CCEH_CoW.o src/trace.o -lpmemobj -ltbb -lpmem -lpthread -lgflags -DMULTITHREAD

test: src/test.cpp CCEH
	$(CXX) $(CFLAGS) -o bin/single_threaded_cceh src/test.cpp src/CCEH.o -lpmemobj -lpmem
	$(CXX) $(CFLAGS) -o bin/multi_threaded_cceh src/test.cpp src/CCEH.o -lpmemobj -lpmem -lpthread -DMULTITHREAD
	$(CXX) $(CFLAGS) -o bin/single_threaded_ccehCoW src/test.cpp src/CCEH_CoW.o -lpmemobj -lpmem
	$(CXX) $(CFLAGS) -o bin/multi_threaded_ccehCoW src/test.cpp src/CCEH_CoW.o -lpmemobj -lpmem -lpthread -DMULTITHREAD

CCEH: src/CCEH.h src/CCEH.cpp
	$(CXX) $(CFLAGS) -c -o src/CCEH.o src/CCEH.cpp -DINPLACE -lpmemobj -lpmem
	$(CXX) $(CFLAGS) -c -o src/CCEH_CoW.o src/CCEH.cpp -lpmemobj -lpmem
	
TRACE: src/trace.h src/trace.cc
	$(CXX) $(CFLAGS) -c -o src/trace.o src/trace.cc

clean:
	rm -rf src/*.o bin/* 

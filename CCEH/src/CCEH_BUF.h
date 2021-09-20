#ifndef CCEH_BUF_H_
#define CCEH_BUF_H_

#include <cstring>
#include <vector>

#include <cmath>
#include <cstdlib>
#include <pthread.h>

#include <libpmemobj.h>
#include "util.h"
#include "../../src/buflog.h"


#define TOID_ARRAY(x) TOID(x)
// 16K / BUFFER_SIZE_FACTOR = BUFFER_SIZE
#define BUFFER_SIZE_FACTOR 4
#define kBufNumMax 1
#define bufferRate 0.7

typedef size_t Key_t;
typedef const char* Value_t;

const Key_t SENTINEL = -2;
const Key_t INVALID = -1;
const Value_t NONE = 0x0;

struct Pair{
    Key_t key;
    Value_t value;
};

class CCEH;
struct Directory;
struct Segment;
struct MemTable;
POBJ_LAYOUT_BEGIN(HashTable); // layout name
POBJ_LAYOUT_ROOT(HashTable, CCEH); // to define a root object
POBJ_LAYOUT_TOID(HashTable, struct Directory); // to define a type that will be used in program
POBJ_LAYOUT_ROOT(HashTable, struct Segment); // to define a root object
POBJ_LAYOUT_TOID(HashTable, TOID(struct Segment));
POBJ_LAYOUT_END(HashTable);

constexpr size_t kSegmentBits = 8;
constexpr size_t kMask = (1 << kSegmentBits) - 1;
constexpr size_t kShift = kSegmentBits;
constexpr size_t kSegmentSize = (1 << kSegmentBits) * 16 * 4; // 16 Bytes * 4 /per Bucket, 1 << kSegmentBits = 256 Buckets
// constexpr size_t kWriteBufferSize = kSegmentSize / 2 / 256; 
constexpr size_t kWriteBufferSize = (kSegmentSize / BUFFER_SIZE_FACTOR / 256) * (1 + 0.3);  // 4K buffer size
constexpr size_t kNumPairPerCacheLine = 4;
constexpr size_t kNumCacheLine = 8; // how many cachelines for linearprobing to search
constexpr size_t kCuckooThreshold = 16;
//constexpr size_t kCuckooThreshold = 32;

// buflog::WriteBuffer<16> *writeBuffer_;

// The way to use the class with template
using WriteBuffer = buflog::WriteBuffer<kWriteBufferSize>;

// to limit the number of buffer in use
extern std::atomic<uint32_t> bufnum;
extern std::atomic<uint32_t> curSegnumNum;

struct Segment{
	// the maximum number of kv pair in Segment
    static const size_t kNumSlot = kSegmentSize/sizeof(Pair);

    Segment(void){ }
    ~Segment(void){ }

    void initSegment(void){
	
		for(int i = 0; i < kNumSlot; ++i) {
			bucket[i].key = INVALID;
		}
		local_depth = 0;
		sema = 0;

		uint32_t requiredBufNum;

		if (kBufNumMax < 0) {
			bufnode_ = new WriteBuffer();
			buf_flag = true;
		} else {
			// The # of buffer depends on the # of segment.
			if (bufferRate > 0) {
				requiredBufNum = bufferRate * curSegnumNum;
			} else {
				requiredBufNum = kBufNumMax;
			}

			uint32_t tmp = bufnum.load(std::memory_order_relaxed);
			if (tmp < requiredBufNum) {
				uint32_t BN = bufnum.fetch_add(1, std::memory_order_relaxed);
				if (BN < requiredBufNum) { 
					// printf("Avaliable buffer : %lu \n", BN);
					bufnode_ = new WriteBuffer();
					buf_flag = true;
				}
			}
		}
		
    }

    void initSegment(size_t depth){

		for(int i = 0; i < kNumSlot; ++i){
			bucket[i].key = INVALID;
		}
		local_depth = depth;
		sema = 0;

		uint32_t requiredBufNum;

		if (kBufNumMax < 0) { // No buffer limit
			bufnode_ = new WriteBuffer(depth);
			buf_flag = true;
		} else {
			// The # of buffer depends on the # of segment.
			if (bufferRate > 0) {
				requiredBufNum = bufferRate * curSegnumNum;
			} else {
				requiredBufNum = kBufNumMax;
			}

			uint32_t tmp = bufnum.load(std::memory_order_relaxed);
			if (tmp < requiredBufNum) {
				uint32_t BN = bufnum.fetch_add(1, std::memory_order_relaxed);
				if (BN < requiredBufNum) { 
					// printf("Avaliable buffer : %lu \n", BN);
					bufnode_ = new WriteBuffer(depth);
					buf_flag = true;
				}
			}
		}
		
    }

    bool suspend(void) {
		int64_t val;
		do {
			val = sema;
			if(val < 0)
				return false;
		} while(!CAS(&sema, &val, -1));

		int64_t wait = 0 - val - 1;
		while (val && sema != wait){
			asm("nop");
		}
		return true;
    }

    bool lock(void) {
		int64_t val = sema;
		while(val > -1){
			if(CAS(&sema, &val, val+1))
			return true;
			val = sema;
		}
		return false;
    }

    void unlock(void){
		int64_t val = sema;
		while(!CAS(&sema, &val, val-1)){
			val = sema;
		}
    }

    int Insert(PMEMobjpool*, Key_t&, Value_t, size_t, size_t);
    bool Insert4split(Key_t&, Value_t, size_t);
    TOID(struct Segment)* Split(PMEMobjpool*);
	/* TODO: Buffer Split*/
	struct Segment* SplitDram(WriteBuffer::Iterator& iter);
    std::vector<std::pair<size_t, size_t>> find_path(size_t, size_t);
    void execute_path(PMEMobjpool*, std::vector<std::pair<size_t, size_t>>&, Key_t&, Value_t);
    void execute_path(std::vector<std::pair<size_t, size_t>>&, Pair);
    size_t numElement(void);

    Pair bucket[kNumSlot];
    int64_t sema = 0;
    size_t local_depth;
    WriteBuffer* bufnode_;
	// the flag to tell this segment has buffer or not
	bool buf_flag = false; 
};

struct Directory{
    static const size_t kDefaultDepth = 10;

    TOID_ARRAY(TOID(struct Segment)) segment;

    int64_t sema = 0;
    size_t capacity;
    size_t depth;

    bool suspend(void){
	int64_t val;
	do{
	    val = sema;
	    if(val < 0)
		return false;
	}while(!CAS(&sema, &val, -1));

	int64_t wait = 0 - val - 1;
	while(val && sema != wait){
	    asm("nop");
	}
	return true;
    }

    bool lock(void){
	int64_t val = sema;
	while(val > -1){
	    if(CAS(&sema, &val, val+1))
		return true;
	    val = sema;
	}
	return false;
    }

    void unlock(void){
		int64_t val = sema;
		while(!CAS(&sema, &val, val-1)){
			val = sema;
		}
    }

    Directory(void){ }
    ~Directory(void){ }

    void initDirectory(void){
		depth = kDefaultDepth;
		capacity = pow(2, depth);
		sema = 0;
    }

    void initDirectory(size_t _depth){
		depth = _depth;
		capacity = pow(2, _depth);
		sema = 0;
    }
};

class CCEH{
public:
	CCEH(void){ }
	~CCEH(void){ }
	void initCCEH(PMEMobjpool*);
	void initCCEH(PMEMobjpool*, size_t);

	/* For CCEH BUF */
	void Insert(PMEMobjpool*, Key_t&, Value_t);
	void insert(PMEMobjpool*, Key_t&, Value_t, bool with_lock);
	void mergeBufAndSplitWhenNeeded(PMEMobjpool*, WriteBuffer* bufnode, Segment_toid& target, size_t x);
	bool InsertOnly(PMEMobjpool*, Key_t&, Value_t);	

	bool Delete(Key_t&);
	Value_t Get(Key_t&);
	Value_t get(Key_t&);
	Value_t FindAnyway(Key_t&);

	double Utilization(void);
	size_t Capacity(void);
	void Recovery(PMEMobjpool*);

	bool crashed = true;

	// record the number of buffer merge to pmem
	uint32_t buffer_writes;
	// record the number of valid key in buffer(when merge to pmem)
	double buffer_kvs;
	double AverageBufLoadFactor(void);

	// check data in segment
	void checkBufferData();


private:
	TOID(struct Directory) dir;
};
#endif

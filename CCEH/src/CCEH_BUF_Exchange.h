#ifndef CCEH_BUF_EXCHANGE_H_
#define CCEH_BUF_EXCHANGE_H_

#include <pthread.h>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <set>
#include <unordered_map>
#include <vector>

#include <libpmemobj.h>
#include <sys/time.h>
#include <time.h>
#include "../../src/buflog.h"
#include "util.h"

#define TOID_ARRAY(x) TOID (x)
// 16K / BUFFER_SIZE_FACTOR = BUFFER_SIZE
// #define BUFFER_SIZE_FACTOR 4
// #define kBufNumMax 1
// #define bufferRate 0.7

typedef size_t Key_t;
typedef const char* Value_t;

const Key_t SENTINEL = -2;
const Key_t INVALID = -1;
const Value_t NONE = 0x0;

struct Pair {
    Key_t key;
    Value_t value;
};

class CCEH;
struct Directory;
struct Segment;
struct MemTable;
POBJ_LAYOUT_BEGIN (HashTable);                   // layout name
POBJ_LAYOUT_ROOT (HashTable, CCEH);              // to define a root object
POBJ_LAYOUT_TOID (HashTable, struct Directory);  // to define a type that will be used in program
POBJ_LAYOUT_ROOT (HashTable, struct Segment);    // to define a root object
POBJ_LAYOUT_TOID (HashTable, TOID (struct Segment));
POBJ_LAYOUT_END (HashTable);

// to limit the number of buffer in use
extern std::atomic<uint32_t> bufnum;
extern std::atomic<uint32_t> curSegnumNum;

constexpr size_t kSegmentBits = 8;
constexpr size_t kMask = (1 << kSegmentBits) - 1;
constexpr size_t kShift = kSegmentBits;
constexpr size_t kSegmentSize =
    (1 << kSegmentBits) * 16 * 4;  // 16 Bytes * 4 /per Bucket, 1 << kSegmentBits = 256 Buckets
// constexpr size_t kWriteBufferSize = kSegmentSize / 2 / 256;
constexpr size_t kWriteBufferSize = (kSegmentSize / 4 / 256) * (1 + 0.3);  // 4K buffer size
constexpr size_t kNumPairPerCacheLine = 4;
constexpr size_t kNumCacheLine = 8;  // how many cachelines for linearprobing to search
constexpr size_t kCuckooThreshold = 16;
// constexpr size_t kCuckooThreshold = 32;

// The way to use the class with template
using WriteBuffer = buflog::WriteBuffer<kWriteBufferSize>;

class BufferConfig {
public:
    std::mutex _BCF_mtx;
    std::mutex set_mutex;  // std::lock_guard<std::mutex> lk (set_mutex); add the lock
    std::set<uint64_t>* bufferContainFlag;
    std::set<uint64_t>* _bufferContainFlag;
    std::set<uint64_t>* bufferNotContainFlag;
    std::set<uint64_t>* _bufferNotContainFlag;
    uint32_t probDistance = 32;
    BufferConfig () {}

    ~BufferConfig (void) {}

    void setBufferSizeFactor (int32_t bufferSizeFactor) { bufferSizeFactor_ = bufferSizeFactor; }
    void setKBufNumMax (int32_t kBufNumMax) { kBufNumMax_ = kBufNumMax; }
    void fetchAddKBufNumMax (int32_t num) { kBufNumMax_ += num; }
    void fetchSubKBufNumMax (int32_t num) { kBufNumMax_ -= num; }
    void setBufferRate (double bufferRate) { bufferRate_ = bufferRate; }

    int32_t getBufferSizeFactor () { return bufferSizeFactor_; }
    int32_t getKBufNumMax () { return kBufNumMax_; }
    double getBufferRate () { return bufferRate_; }

    void initDirEntryMapping (uint64_t initSize) {
        currentHashCap = initSize;
        // Can't del directory, consider other thread is reading ..
        dirEntryMapping = new uint64_t[initSize];
        memset (dirEntryMapping, 0, initSize * sizeof (uint64_t));
    }
    uint64_t getDirEntryMapping () {
        // return an entry of dir
        auto randN = rand ();
        for (uint32_t i = 0; i < probDistance; i++) {
            auto loc = (randN + i) % currentHashCap;
            if (dirEntryMapping[loc] != 0) {
                // TODO : delete ??
                return loc;
            }
        }
        return randN % currentHashCap;
    }

    void setDirEntryMapping (uint64_t index) {
        for (uint32_t i = 0; i < probDistance; i++) {
            auto loc = (index + i) % currentHashCap;
            if (dirEntryMapping[loc] == 0) {
                dirEntryMapping[loc] = index;
                return;
            }
        }
        // TODO : Collision
    }

    void removeDirEntryMapping (uint64_t index) {
        for (uint32_t i = 0; i < probDistance; i++) {
            auto loc = (index + i) % currentHashCap;
            if (dirEntryMapping[loc] == index) {
                dirEntryMapping[loc] = 0;
                return;
            }
        }
        // Didn't find the index means there is no entry
    }

private:
    int32_t bufferSizeFactor_;
    int32_t kBufNumMax_;
    double bufferRate_;

    uint64_t currentHashCap;
    uint64_t* dirEntryMapping;
};

class Timeval {
public:
    uint64_t start_;
    uint64_t end_;
    uint64_t interval_;

    uint64_t NowMicros () {
        static constexpr uint64_t kUsecondsPerSecond = 1000000;
        struct ::timeval tv;
        ::gettimeofday (&tv, nullptr);
        return static_cast<uint64_t> (tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
    }
};

struct Segment {
    // the maximum number of kv pair in Segment
    static const size_t kNumSlot = kSegmentSize / sizeof (Pair);

    Segment (void) {}
    ~Segment (void) {}

    int initSegment (CCEH*);
    int initSegment (size_t, CCEH*);

    bool suspend (void) {
        int64_t val;
        do {
            val = sema;
            if (val < 0) return false;
        } while (!CAS (&sema, &val, -1));

        int64_t wait = 0 - val - 1;
        while (val && sema != wait) {
            asm("nop");
        }
        return true;
    }

    bool lock (void) {
        int64_t val = sema;
        while (val > -1) {
            if (CAS (&sema, &val, val + 1)) return true;
            val = sema;
        }
        return false;
    }

    void unlock (void) {
        int64_t val = sema;
        while (!CAS (&sema, &val, val - 1)) {
            val = sema;
        }
    }

    int Insert (PMEMobjpool*, Key_t&, Value_t, size_t, size_t);
    bool Insert4split (Key_t&, Value_t, size_t);
    TOID (struct Segment) * Split (PMEMobjpool*);
    /* TODO: Buffer Split*/
    struct Segment* SplitDram (WriteBuffer::Iterator& iter);
    std::vector<std::pair<size_t, size_t>> find_path (size_t, size_t);
    void execute_path (PMEMobjpool*, std::vector<std::pair<size_t, size_t>>&, Key_t&, Value_t);
    void execute_path (std::vector<std::pair<size_t, size_t>>&, Pair);
    size_t numElement (void);

    Pair bucket[kNumSlot];
    int64_t sema = 0;
    size_t local_depth;
    WriteBuffer* bufnode_;
    // the flag to tell this segment has buffer or not
    bool buf_flag = false;
    uint32_t nWriteBuffers;

    uint64_t startt_;
    uint64_t endt_;
    uint64_t interval_;

    bool bufferDestroy;

    CCEH* cceh;
    Timeval timeval;
};

struct Directory {
    static const size_t kDefaultDepth = 10;

    TOID_ARRAY (TOID (struct Segment)) segment;

    int64_t sema = 0;
    size_t capacity;
    size_t depth;

    bool suspend (void) {
        int64_t val;
        do {
            val = sema;  // check other threads is using or not.
            if (val < 0) return false;
        } while (!CAS (&sema, &val, -1));

        int64_t wait = 0 - val - 1;
        while (val && sema != wait) {
            asm("nop");
        }
        return true;
    }

    bool lock (void) {
        int64_t val = sema;
        while (val > -1) {
            if (CAS (&sema, &val, val + 1)) return true;
            val = sema;
        }
        return false;
    }

    void unlock (void) {
        int64_t val = sema;
        while (!CAS (&sema, &val, val - 1)) {
            val = sema;
        }
    }

    Directory (void) {}
    ~Directory (void) {}

    void initDirectory (void) {
        depth = kDefaultDepth;
        capacity = pow (2, depth);
        sema = 0;
    }

    void initDirectory (size_t _depth) {
        depth = _depth;
        capacity = pow (2, _depth);
        sema = 0;
    }
};

class CCEH {
public:
    CCEH (void) {}
    ~CCEH (void) {}
    void initCCEH (PMEMobjpool*);
    void initCCEH (PMEMobjpool*, size_t);
    void initCCEH (PMEMobjpool*, size_t, uint32_t, uint32_t, double);

    /* For CCEH BUF */
    // return val indicates whether the insert trigers the minor compaction just now.
    bool Insert (PMEMobjpool*, Key_t&, Value_t);
    void insert (PMEMobjpool*, Key_t&, Value_t, bool with_lock);
    void mergeBufAndSplitWhenNeeded (PMEMobjpool*, WriteBuffer* bufnode, Segment_toid& target,
                                     size_t x);
    bool InsertOnly (PMEMobjpool*, Key_t&, Value_t);

    bool Delete (Key_t&);
    Value_t Get (Key_t&);
    Value_t get (Key_t&);
    Value_t FindAnyway (Key_t&);

    double Utilization (void);
    size_t Capacity (void);
    void Recovery (PMEMobjpool*);

    bool crashed = true;

    // record the number of buffer merge to pmem
    uint32_t bufferWrites;

    // check data in segment
    void checkBufferData ();

    bool releaseBuffer (PMEMobjpool*);
    bool addBuffer ();

    BufferConfig bufferConfig;

    // to limit the number of buffer in use
    std::atomic<uint32_t> bufnum;
    std::atomic<uint32_t> curSegnumNum;

private:
    TOID (struct Directory) dir;
};
#endif
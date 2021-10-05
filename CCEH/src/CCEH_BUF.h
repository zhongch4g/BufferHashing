#ifndef CCEH_BUF_H_
#define CCEH_BUF_H_

#include <cstring>
#include <vector>

#include <pthread.h>
#include <cmath>
#include <cstdlib>

#include <libpmemobj.h>
#include "../../src/buflog.h"
#include "util.h"

#define TOID_ARRAY(x) TOID (x)

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
    BufferConfig () {}

    ~BufferConfig (void) {}

    void setBufferSizeFactor (int32_t bufferSizeFactor) { bufferSizeFactor_ = bufferSizeFactor; }
    void setKBufNumMax (int32_t kBufNumMax) { kBufNumMax_ = kBufNumMax; }
    void setBufferRate (double bufferRate) { bufferRate_ = bufferRate; }

    int32_t getBufferSizeFactor () { return bufferSizeFactor_; }
    int32_t getKBufNumMax () { return kBufNumMax_; }
    double getBufferRate () { return bufferRate_; }
    void printConfig () {
        printf (
            "| Buffer Size Factor | BufNumMax | BufferRate | \n \
                |        %d       |     %d    |   %1.1f    | \n ",
            getBufferSizeFactor (), getKBufNumMax (), getBufferRate ());
    }

private:
    int32_t bufferSizeFactor_;
    int32_t kBufNumMax_;
    double bufferRate_;
};

struct Segment {
    // the maximum number of kv pair in Segment
    static const size_t kNumSlot = kSegmentSize / sizeof (Pair);

    Segment (void) {}
    ~Segment (void) {}

    void initSegment (CCEH*);
    void initSegment (size_t, CCEH*);

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
    void releaseBuffer ();

    Pair bucket[kNumSlot];
    int64_t sema = 0;
    size_t local_depth;
    WriteBuffer* bufnode_;
    // the flag to tell this segment has buffer or not
    bool buf_flag = false;

    CCEH* cceh;
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
    void Insert (PMEMobjpool*, Key_t&, Value_t);
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

    uint32_t bufferWrites;
    // check data in segment
    void checkBufferData ();

    BufferConfig bufferConfig;
    // to limit the number of buffer in use
    std::atomic<uint32_t> bufnum;
    std::atomic<uint32_t> curSegnumNum;

private:
    TOID (struct Directory) dir;
};
#endif
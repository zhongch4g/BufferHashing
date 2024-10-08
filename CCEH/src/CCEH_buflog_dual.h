#ifndef CCEH_BUFLOG_DUAL_H_
#define CCEH_BUFLOG_DUAL_H_

#include <libpmemobj.h>
#include <pthread.h>

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "../../src/buflog.h"
#include "logger.h"
#include "util.h"

#define TOID_ARRAY(x) TOID (x)

namespace cceh_buflog_dual {

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
POBJ_LAYOUT_BEGIN (HashTable);
POBJ_LAYOUT_ROOT (HashTable, CCEH);
POBJ_LAYOUT_TOID (HashTable, struct Directory);
POBJ_LAYOUT_ROOT (HashTable, struct Segment);
POBJ_LAYOUT_TOID (HashTable, TOID (struct Segment));
POBJ_LAYOUT_END (HashTable);

constexpr size_t kSegmentBits = 8;
constexpr size_t kMask = (1 << kSegmentBits) - 1;
constexpr size_t kShift = kSegmentBits;
constexpr size_t kSegmentSize = (1 << kSegmentBits) * 16 * 4;
constexpr size_t kWriteBufferSize = kSegmentSize / 2 / 256 * (1 + 0.3);
constexpr size_t kNumPairPerCacheLine = 4;
constexpr size_t kNumCacheLine = 8;
constexpr size_t kCuckooThreshold = 16;

extern std::atomic<int32_t> validBuffer;
extern std::atomic<int32_t> validBuffer_;
extern bool validBufferFlag;
using WriteBuffer = buflog::WriteBuffer;

class BufferConfig {
public:
    std::unordered_set<uint32_t>* noBufferIndexSet;
    std::mutex IndexSetLock;

    BufferConfig () {}
    ~BufferConfig (void) {}
    void setBufferSizeFactor (int32_t bufferSizeFactor) { bufferSizeFactor_ = bufferSizeFactor; }
    void setKBufNumMax (int32_t kBufNumMax) { kBufNumMax_ = kBufNumMax; }
    void fetchAddKBufNumMax (int32_t num) { kBufNumMax_ += num; }
    void fetchSubKBufNumMax (int32_t num) { kBufNumMax_ -= num; }
    void setNumSplit (int32_t n) { kNumSplit = n; }
    void fetchAddNumSplit (int32_t n) { kNumSplit += n; }
    int32_t getNumSplit () { return kNumSplit; }

    int32_t getBufferSizeFactor () { return bufferSizeFactor_; }
    int32_t getKBufNumMax () { return kBufNumMax_; }

private:
    int32_t bufferSizeFactor_;
    int32_t kBufNumMax_;
    int32_t kNumSplit;
};

struct Segment {
    static const size_t kNumSlot = kSegmentSize / sizeof (Pair);

    Segment (void) {}
    ~Segment (void) {}
    void initSegment (void) {
        for (int i = 0; i < kNumSlot; ++i) {
            bucket[i].key = INVALID;
        }
        local_depth = 0;
        sema = 0;
    }

    void initSegment (size_t depth) {
        for (int i = 0; i < kNumSlot; ++i) {
            bucket[i].key = INVALID;
        }
        local_depth = depth;
        sema = 0;
    }
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
    struct Segment* SplitDram (WriteBuffer::Iterator& iter);
    std::vector<std::pair<size_t, size_t>> find_path (size_t, size_t);
    void execute_path (PMEMobjpool*, std::vector<std::pair<size_t, size_t>>&, Key_t&, Value_t);
    void execute_path (std::vector<std::pair<size_t, size_t>>&, Pair);
    size_t numElement (void);

    Pair bucket[kNumSlot];
    int64_t sema = 0;
    size_t local_depth;
};

struct intermediate {
    WriteBuffer* bufnode;
};

struct Directory {
    static const size_t kDefaultDepth = 10;

    TOID_ARRAY (TOID (struct Segment)) segment;
    // WriteBuffer** bufnodes;
    intermediate** intermediates;

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
        // INFO ("Directory capacity: %lu. depth %lu\n", capacity, depth);
        // printf ("Directory capacity: %lu. depth %lu\n", capacity, depth);
    }

    void initDirectory (size_t _depth) {
        depth = _depth;
        capacity = pow (2, _depth);
        // INFO ("Directory capacity: %lu. depth %lu\n", capacity, depth);
        // printf ("Directory capacity: %lu. depth %lu\n", capacity, depth);
        sema = 0;
    }
};

class CCEH {
public:
    CCEH (void) {}
    ~CCEH (void) {}
    void initCCEH (PMEMobjpool*);
    void initCCEH (PMEMobjpool*, size_t, uint32_t);

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
    void transferBuffer (void);
    void forceReleaseAllBuffers (PMEMobjpool*, TOID (CCEH));
    void flushAllBuffers (PMEMobjpool*);
    void BufferCheck (void);
    bool increaseBuffer (void);
    void releaseAllBuffers (void);
    void acceptAllBuffers (void);

    BufferConfig bufferConfig;           // to record the basic buffer configuration
    std::atomic<uint32_t> curBufferNum;  // to record current # of buffer
    std::atomic<uint32_t> curSegmentNum;
    std::atomic<int32_t> balance;

private:
    TOID (struct Directory) dir;
};
};  // namespace cceh_buflog_dual
#endif

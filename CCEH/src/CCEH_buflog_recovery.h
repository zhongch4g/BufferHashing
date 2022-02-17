#ifndef CCEH_H_
#define CCEH_H_

#include <libpmemobj.h>
#include <pthread.h>

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <vector>

#include "../../src/buflog.h"
#include "logger.h"
#include "util.h"

#define TOID_ARRAY(x) TOID (x)

typedef size_t Key_t;
typedef const char *Value_t;

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
constexpr size_t kWriteBufferSize = (kSegmentSize / 2 / 256) * (1 + 0.3);
constexpr size_t kNumPairPerCacheLine = 4;
constexpr size_t kNumCacheLine = 8;
constexpr size_t kCuckooThreshold = 16;

using WriteBuffer = buflog::WriteBuffer<kWriteBufferSize>;
// constexpr size_t kCuckooThreshold = 32;

struct Segment {
    static const size_t kNumSlot = kSegmentSize / sizeof (Pair);

    Segment (void) {}
    ~Segment (void) {}

    void initSegment (pmem::obj::pool<CCEH> pop) {
        for (int i = 0; i < kNumSlot; ++i) {
            bucket[i].key = INVALID;
        }
        local_depth = 0;
        sema = 0;
        bufnode_ = new WriteBuffer ();
        pmem::obj::transaction::run (pop, [&] () {
            logPtr = pmem::obj::make_persistent<buflog::LogPtr> ();
            logPtr.get ()->initLogPtr ();
        });
        recovery = false;
    }

    void initSegment (pmem::obj::pool<CCEH> pop, size_t depth) {
        for (int i = 0; i < kNumSlot; ++i) {
            bucket[i].key = INVALID;
        }
        local_depth = depth;
        sema = 0;
        bufnode_ = new WriteBuffer (depth);
        pmem::obj::transaction::run (pop, [&] () {
            logPtr = pmem::obj::make_persistent<buflog::LogPtr> ();
            logPtr->initLogPtr ();
        });
        recovery = false;
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

    int Insert (PMEMobjpool *, Key_t &, Value_t, size_t, size_t);
    bool Insert4split (Key_t &, Value_t, size_t);
    TOID (struct Segment) * Split (pmem::obj::pool<CCEH> pop);
    struct Segment *SplitDram (pmem::obj::pool<CCEH> pop, WriteBuffer::Iterator &iter);
    std::vector<std::pair<size_t, size_t>> find_path (size_t, size_t);
    void execute_path (PMEMobjpool *, std::vector<std::pair<size_t, size_t>> &, Key_t &, Value_t);
    void execute_path (std::vector<std::pair<size_t, size_t>> &, Pair);
    size_t numElement (void);
    bool recovery;
    Pair bucket[kNumSlot];
    int64_t sema = 0;
    size_t local_depth;
    WriteBuffer *bufnode_;
    pmem::obj::persistent_ptr<buflog::LogPtr> logPtr;
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
        printf ("Directory capacity: %lu. depth %lu\n", capacity, depth);
    }

    void initDirectory (size_t _depth) {
        depth = _depth;
        capacity = pow (2, _depth);
        printf ("Directory capacity: %lu. depth %lu\n", capacity, depth);
        sema = 0;
    }
};

class CCEH {
public:
    CCEH (void) {}
    ~CCEH (void) {}
    void initCCEH (pmem::obj::pool<CCEH>);
    void initCCEH (pmem::obj::pool<CCEH>, size_t, size_t);

    void InsertWithLog (pmem::obj::pool<CCEH>, Key_t &, Value_t, int);
    void insert (pmem::obj::pool<CCEH>, Key_t &, Value_t, bool with_lock);
    void mergeBufAndSplitWhenNeeded (pmem::obj::pool<CCEH>, WriteBuffer *bufnode,
                                     Segment_toid &target, size_t x);
    bool InsertOnly (pmem::obj::pool<CCEH>, Key_t &, Value_t);
    bool Delete (Key_t &);
    Value_t Get (Key_t &);
    Value_t get (Key_t &);
    Value_t FindAnyway (Key_t &);

    double Utilization (void);
    size_t Capacity (void);
    void Recovery (pmem::obj::pool<CCEH>, size_t, std::vector<uint64_t> &);
    void BufferRecovery2 (pmem::obj::pool<CCEH>, uint64_t, int, std::vector<uint64_t>);
    void BufferRecovery1 (pmem::obj::pool<CCEH>, uint64_t);

    bool crashed = true;
    pmem::obj::persistent_ptr<buflog::linkedredolog::BufferLogNode[]> BufferLogNodes;

private:
    TOID (struct Directory) dir;
};

#endif

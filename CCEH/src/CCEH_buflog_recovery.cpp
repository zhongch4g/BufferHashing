#include "CCEH_buflog_recovery.h"

#include <stdio.h>

#include <bitset>
#include <cassert>
#include <iostream>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <thread>
#include <unordered_map>
#include <vector>

#include "hash.h"
#include "logger.h"
#include "util.h"
using namespace std;
#define f_seed 0xc70697UL
#define s_seed 0xc70697UL
//#define f_seed 0xc70f6907UL
//#define s_seed 0xc70f6907UL

#define INPLACE

// #define WITHOUT_FLUSH

#define CONFIG_OUT_OF_PLACE_MERGE

using namespace std;
using namespace buflog_recovery;

void Segment::execute_path (PMEMobjpool *pop, vector<pair<size_t, size_t>> &path, Key_t &key,
                            Value_t value) {
    for (int i = path.size () - 1; i > 0; --i) {
        bucket[path[i].first] = bucket[path[i - 1].first];
        pmemobj_persist (pop, (char *)&bucket[path[i].first], sizeof (Pair));
    }
    bucket[path[0].first].value = value;
    mfence ();
    bucket[path[0].first].key = key;
    pmemobj_persist (pop, (char *)&bucket[path[0].first], sizeof (Pair));
}

void Segment::execute_path (vector<pair<size_t, size_t>> &path, Pair _bucket) {
    int i = 0;
    int j = (i + 1) % 2;

    Pair temp[2];
    temp[0] = _bucket;
    for (auto p : path) {
        temp[j] = bucket[p.first];
        bucket[p.first] = temp[i];
        i = (i + 1) % 2;
        j = (i + 1) % 2;
    }
}

vector<pair<size_t, size_t>> Segment::find_path (size_t target, size_t pattern) {
    vector<pair<size_t, size_t>> path;
    path.reserve (kCuckooThreshold);
    path.emplace_back (target, bucket[target].key);

    auto cur = target;
    int i = 0;

    do {
        Key_t *key = &bucket[cur].key;
        auto f_hash = hash_funcs[0](key, sizeof (Key_t), f_seed);
        auto s_hash = hash_funcs[2](key, sizeof (Key_t), s_seed);

        if ((f_hash >> (8 * sizeof (f_hash) - local_depth)) != pattern || *key == INVALID) {
            break;
        }

        for (int j = 0; j < kNumPairPerCacheLine * kNumCacheLine; ++j) {
            auto f_idx = (((f_hash & kMask) * kNumPairPerCacheLine) + j) % kNumSlot;
            auto s_idx = (((s_hash & kMask) * kNumPairPerCacheLine) + j) % kNumSlot;

            if (f_idx == cur) {
                path.emplace_back (s_idx, bucket[s_idx].key);
                cur = s_idx;
                break;
            } else if (s_idx == cur) {
                path.emplace_back (f_idx, bucket[f_idx].key);
                cur = f_idx;
                break;
            }
        }
        ++i;
    } while (i < kCuckooThreshold);

    if (i == kCuckooThreshold) {
        path.resize (0);
    }

    return move (path);
}

bool Segment::Insert4split (Key_t &key, Value_t value, size_t loc) {
    for (int i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto slot = (loc + i) % kNumSlot;
        if (bucket[slot].key == INVALID) {
            bucket[slot].key = key;
            bucket[slot].value = value;
            return 1;
        }
    }
    return 0;
}

Segment *Segment::SplitDram (pmem::obj::pool<CCEH> pop, WriteBuffer::Iterator &iter) {
    Segment *split = new Segment ();
    // splits[0].initSegment(local_depth+1); //   old segment
    split->initSegment (pop, local_depth + 1);  // split segment

    auto pattern = ((size_t)1 << (sizeof (Key_t) * 8 - local_depth - 1));

    for (int i = 0; i < kNumSlot; ++i) {
        auto f_hash = h (&bucket[i].key, sizeof (Key_t));
        if (f_hash & pattern) {
            if (!split->Insert4split (bucket[i].key, bucket[i].value,
                                      (f_hash & kMask) * kNumPairPerCacheLine)) {
                auto s_hash = hash_funcs[2](&bucket[i].key, sizeof (Key_t), s_seed);
                if (!split->Insert4split (bucket[i].key, bucket[i].value,
                                          (s_hash & kMask) * kNumPairPerCacheLine)) {
                    INFO ("S hash 1 insert split segment fail");
                }
            }
            // invalidate the migrated key
            bucket[i].key = INVALID;
        }
    }

    while (iter.Valid ()) {
        auto &kv = *iter;
        Key_t key = kv.key;
        Value_t val = kv.val;
        auto f_hash = h (&key, sizeof (Key_t));
        if (f_hash & pattern) {
            // insert to split segment
            if (!split->Insert4split (key, val, (f_hash & kMask) * kNumPairPerCacheLine)) {
                auto s_hash = hash_funcs[2](&key, sizeof (Key_t), s_seed);
                if (!split->Insert4split (key, val, (s_hash & kMask) * kNumPairPerCacheLine)) {
                    INFO ("S hash iter 1 insert fail.");
                }
            }
        } else {
            // insert to this original segment
            if (!Insert4split (key, val, (f_hash & kMask) * kNumPairPerCacheLine)) {
                auto s_hash = hash_funcs[2](&key, sizeof (Key_t), s_seed);
                if (!Insert4split (key, val, (s_hash & kMask) * kNumPairPerCacheLine)) {
                    INFO ("S hash iter 0 insert fail");
                }
            }
        }
        ++iter;
    }

    return split;
}

TOID (struct Segment) * Segment::Split (pmem::obj::pool<CCEH> pop) {
#ifdef INPLACE
    TOID (struct Segment) *split = new TOID (struct Segment)[2];
    split[0] = pmemobj_oid (this);
    POBJ_ALLOC (pop.handle (), &split[1], struct Segment, sizeof (struct Segment), NULL, NULL);
    D_RW (split[1])->initSegment (pop, local_depth + 1);

    auto pattern = ((size_t)1 << (sizeof (Key_t) * 8 - local_depth - 1));

    for (int i = 0; i < kNumSlot; ++i) {
        auto f_hash = hash_funcs[0](&bucket[i].key, sizeof (Key_t), f_seed);
        if (f_hash & pattern) {
            if (!D_RW (split[1])->Insert4split (bucket[i].key, bucket[i].value,
                                                (f_hash & kMask) * kNumPairPerCacheLine)) {
                auto s_hash = hash_funcs[2](&bucket[i].key, sizeof (Key_t), s_seed);
                if (!D_RW (split[1])->Insert4split (bucket[i].key, bucket[i].value,
                                                    (s_hash & kMask) * kNumPairPerCacheLine)) {
                }
            }
        }
    }

    pmemobj_persist (pop.handle (), (char *)D_RO (split[1]), sizeof (struct Segment));
    return split;
#else
    TOID (struct Segment) *split = new TOID (struct Segment)[2];
    POBJ_ALLOC (pop.handle (), &split[0], struct Segment, sizeof (struct Segment), NULL, NULL);
    POBJ_ALLOC (pop.handle (), &split[1], struct Segment, sizeof (struct Segment), NULL, NULL);
    D_RW (split[0])->initSegment (pop, local_depth + 1);
    D_RW (split[1])->initSegment (pop, local_depth + 1);

    auto pattern = ((size_t)1 << (sizeof (Key_t) * 8 - local_depth - 1));
    for (int i = 0; i < kNumSlot; ++i) {
        auto f_hash = h (&bucket[i].key, sizeof (Key_t));
        if (f_hash & pattern) {
            D_RW (split[1])->Insert4split (bucket[i].key, bucket[i].value,
                                           (f_hash & kMask) * kNumPairPerCacheLine);
        } else {
            D_RW (split[0])->Insert4split (bucket[i].key, bucket[i].value,
                                           (f_hash & kMask) * kNumPairPerCacheLine);
        }
    }

    pmemobj_persist (pop.handle (), (char *)D_RO (split[0]), sizeof (struct Segment));
    pmemobj_persist (pop.handle (), (char *)D_RO (split[1]), sizeof (struct Segment));

    return split;
#endif
}

void CCEH::initCCEH (pmem::obj::pool<CCEH> pop) {
    crashed = true;
    POBJ_ALLOC (pop.handle (), &dir, struct Directory, sizeof (struct Directory), NULL, NULL);
    D_RW (dir)->initDirectory ();
    POBJ_ALLOC (pop.handle (), &D_RW (dir)->segment, TOID (struct Segment),
                sizeof (TOID (struct Segment)) * D_RO (dir)->capacity, NULL, NULL);

    for (int i = 0; i < D_RO (dir)->capacity; ++i) {
        POBJ_ALLOC (pop.handle (), &D_RO (D_RO (dir)->segment)[i], struct Segment,
                    sizeof (struct Segment), NULL, NULL);
        D_RW (D_RW (D_RW (dir)->segment)[i])->initSegment (pop);
    }
}

void CCEH::initCCEH (pmem::obj::pool<CCEH> pop, size_t initCap, size_t nthreads) {
    crashed = true;
    POBJ_ALLOC (pop.handle (), &dir, struct Directory, sizeof (struct Directory), NULL, NULL);
    D_RW (dir)->initDirectory (static_cast<size_t> (log2 (initCap)));
    POBJ_ALLOC (pop.handle (), &D_RW (dir)->segment, TOID (struct Segment),
                sizeof (TOID (struct Segment)) * D_RO (dir)->capacity, NULL, NULL);

    for (int i = 0; i < D_RO (dir)->capacity; ++i) {
        POBJ_ALLOC (pop.handle (), &D_RO (D_RO (dir)->segment)[i], struct Segment,
                    sizeof (struct Segment), NULL, NULL);
        D_RW (D_RW (D_RW (dir)->segment)[i])
            ->initSegment (pop, static_cast<size_t> (log2 (initCap)));
    }
    pmem::obj::transaction::run (pop, [&] () {
        // Get the base address of the pool
        void *baseAddr = pop.handle ();
        INFO ("The base address = %p \n", baseAddr);
        if (BufferLogNodes == nullptr) {
            pop.root ()->BufferLogNodes =
                pmem::obj::make_persistent<buflog::linkedredolog::BufferLogNode[]> (nthreads);
            for (size_t j = 0; j < nthreads; j++) {
                pop.root ()->BufferLogNodes[j].Create (1024 * 1024 * 1024 * 1);
                DEBUG ("Log[%d] create successfully \n", j);
            }
        }
    });
}

void CCEH::InsertWithLog (pmem::obj::pool<CCEH> pop, Key_t &key, Value_t value, int tid) {
    auto f_hash = hash_funcs[0](&key, sizeof (Key_t), f_seed);

retry:
    auto x = (f_hash >> (8 * sizeof (f_hash) - D_RO (dir)->depth));
    auto target = D_RO (D_RO (dir)->segment)[x];
    auto target_ptr = D_RW (target);

    // quick restart
    if (false && !target_ptr->recovery) {
        // put the linked buffer data into log first
        // DEBUG ("logptr -> %lu \n", ptr->logPtr->getData ());
        uint64_t logid = target_ptr->logPtr->getLogId ();
        uint64_t offset = target_ptr->logPtr->getOffset ();
        target_ptr->bufnode_ = new WriteBuffer (target_ptr->local_depth, 32 + (1 + 0.3));

        auto iterRecover = pop.root ()->BufferLogNodes[logid].lBeginRecover (offset);
        if (iterRecover.Valid ()) {
            while (!iterRecover.IsCheckPoint ()) {
                auto key = (uint64_t)(iterRecover.currentAddr () - 16);
                auto val = (uint64_t)(iterRecover.currentAddr () - 8);
                target_ptr->bufnode_->Put (key, reinterpret_cast<char *> (val));
                iterRecover++;
            }
        }
        target_ptr->recovery = true;
    }

    target_ptr->bufnode_->Lock ();

    if (D_RO (target)->local_depth != target_ptr->bufnode_->local_depth) {
        target_ptr->bufnode_->Unlock ();
        std::this_thread::yield ();
        goto retry;
    }

    bool res = target_ptr->bufnode_->PutWithLog (key, (char *)value);

    if (res) {
        pmem::obj::transaction::run (pop, [&] () {
            auto data = target_ptr->logPtr->getData ();
            auto next_ = 0;
            // data == 256 means it is the first time to use this buffer
            if (data == 256) {
                next_ = pop.root ()->BufferLogNodes[tid].Append (buflog::kDataLogNodeCheckpoint,
                                                                 key, (uint64_t)value, data, true);
            } else {
                next_ = pop.root ()->BufferLogNodes[tid].Append (buflog::kDataLogNodeValid, key,
                                                                 (uint64_t)value, data, true);
            }
            target_ptr->logPtr->setData (tid, next_);
        });

        // auto data = target_ptr->logPtr->getData ();
        // auto next_ = 0;
        // // data == 256 means it is the first time to use this buffer
        // if (data == 256) {
        //     next_ = pop.root ()->BufferLogNodes[tid].Append (buflog::kDataLogNodeCheckpoint, key,
        //                                                      (uint64_t)value, data, true);
        // } else {
        //     next_ = pop.root ()->BufferLogNodes[tid].Append (buflog::kDataLogNodeValid, key,
        //                                                      (uint64_t)value, data, true);
        // }
        // target_ptr->logPtr->setData (tid, next_);
        // // pop.root ()->BufferLogNodes.persist ();
        // target_ptr->logPtr.persist ();

        // successfully insert to bufnode
        target_ptr->bufnode_->Unlock ();
    } else {
        // transaction::run (pop, [&] () {
        //     // next_ : the current data's position
        //     auto next_ = pop.root ()->BufferLogNodes[tid].Append (
        //         buflog::kDataLogNodeCheckpoint, key, (uint64_t)value,
        //         target_ptr->logPtr->getData (), true);
        //     // record new insert data's position in buffer, format as (tid + offset in log)
        //     target_ptr->logPtr->setData (tid, next_);
        //     DEBUG ("th %d   next_ %lu \n", tid, next_);
        // });

        // bufnode is full. merge to CCEH
#ifndef CONFIG_OUT_OF_PLACE_MERGE
        auto iter = D_RW (target)->bufnode_->Begin ();
        while (iter.Valid ()) {
            auto &kv = *iter;
            Key_t key = kv.key;
            Value_t val = kv.val;
            insert (pop, key, val, false);
            ++iter;
        }
#ifdef WITHOUT_FLUSH
        pmemobj_persist (pop, (char *)&D_RO (D_RO (dir)->segment)[x],
                         sizeof (TOID (struct Segment)));
#endif
        D_RW (target)->bufnode_->Reset ();
        D_RW (target)->bufnode_->Unlock ();
        goto retry;
#else

        mergeBufAndSplitWhenNeeded (pop, target_ptr->bufnode_, target, f_hash);

        target_ptr->bufnode_->Unlock ();

        goto retry;
#endif
    }
}

void CCEH::insert (pmem::obj::pool<CCEH> pop, Key_t &key, Value_t value, bool with_lock) {
    auto f_hash = hash_funcs[0](&key, sizeof (Key_t), f_seed);
    auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;

RETRY:
    auto x = (f_hash >> (8 * sizeof (f_hash) - D_RO (dir)->depth));
    auto target = D_RO (D_RO (dir)->segment)[x];

    if (!D_RO (target)) {
        std::this_thread::yield ();
        goto RETRY;
    }

    if (with_lock) {
        /* acquire segment exclusive lock */
        if (!D_RW (target)->lock ()) {
            std::this_thread::yield ();
            goto RETRY;
        }
    }

    auto target_check = (f_hash >> (8 * sizeof (f_hash) - D_RO (dir)->depth));
    if (D_RO (target) != D_RO (D_RO (D_RO (dir)->segment)[target_check])) {
        if (with_lock) D_RW (target)->unlock ();
        std::this_thread::yield ();
        goto RETRY;
    }

    auto pattern = (f_hash >> (8 * sizeof (f_hash) - D_RO (target)->local_depth));
    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto loc = (f_idx + i) % Segment::kNumSlot;
        auto _key = D_RO (target)->bucket[loc].key;
        /* validity check for entry keys */
        if ((((hash_funcs[0](&D_RO (target)->bucket[loc].key, sizeof (Key_t), f_seed) >>
               (8 * sizeof (f_hash) - D_RO (target)->local_depth)) != pattern) ||
             (D_RO (target)->bucket[loc].key == INVALID)) &&
            (D_RO (target)->bucket[loc].key != SENTINEL)) {
            if (CAS (&D_RW (target)->bucket[loc].key, &_key, SENTINEL)) {
                D_RW (target)->bucket[loc].value = value;
#ifndef WITHOUT_FLUSH
                mfence ();
#endif
                D_RW (target)->bucket[loc].key = key;
#ifndef WITHOUT_FLUSH
                pmemobj_persist (pop.handle (), (char *)&D_RO (target)->bucket[loc], sizeof (Pair));
#endif
                /* release segment exclusive lock */
                if (with_lock) D_RW (target)->unlock ();
                return;
            }
        }
    }

    auto s_hash = hash_funcs[2](&key, sizeof (Key_t), s_seed);
    auto s_idx = (s_hash & kMask) * kNumPairPerCacheLine;

    for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto loc = (s_idx + i) % Segment::kNumSlot;
        auto _key = D_RO (target)->bucket[loc].key;
        if ((((hash_funcs[0](&D_RO (target)->bucket[loc].key, sizeof (Key_t), f_seed) >>
               (8 * sizeof (s_hash) - D_RO (target)->local_depth)) != pattern) ||
             (D_RO (target)->bucket[loc].key == INVALID)) &&
            (D_RO (target)->bucket[loc].key != SENTINEL)) {
            if (CAS (&D_RW (target)->bucket[loc].key, &_key, SENTINEL)) {
                D_RW (target)->bucket[loc].value = value;
#ifndef WITHOUT_FLUSH
                mfence ();
#endif
                D_RW (target)->bucket[loc].key = key;
#ifndef WITHOUT_FLUSH
                pmemobj_persist (pop.handle (), (char *)&D_RO (target)->bucket[loc], sizeof (Pair));
#endif
                if (with_lock) D_RW (target)->unlock ();
                return;
            }
        }
    }

    auto target_local_depth = D_RO (target)->local_depth;
    // COLLISION !!
    /* need to split segment but release the exclusive lock first to avoid
     * deadlock */
    if (with_lock) D_RW (target)->unlock ();

    if (with_lock && !D_RW (target)->suspend ()) {
        std::this_thread::yield ();
        goto RETRY;
    }

    /* need to check whether the target segment has been split */
#ifdef INPLACE
    if (target_local_depth != D_RO (target)->local_depth) {
        D_RW (target)->sema = 0;
        std::this_thread::yield ();
        goto RETRY;
    }
#else
    if (target_local_depth != D_RO (D_RO (D_RO (dir)->segment)[x])->local_depth) {
        D_RW (target)->sema = 0;
        std::this_thread::yield ();
        goto RETRY;
    }
#endif

    TOID (struct Segment) *s = D_RW (target)->Split (pop);
DIR_RETRY:
    /* need to double the directory */
    if (D_RO (target)->local_depth == D_RO (dir)->depth) {
        if (!D_RW (dir)->suspend ()) {
            std::this_thread::yield ();
            goto DIR_RETRY;
        }
        printf ("Double dir\n");

        x = (f_hash >> (8 * sizeof (f_hash) - D_RO (dir)->depth));
        auto dir_old = dir;
        TOID_ARRAY (TOID (struct Segment)) d = D_RO (dir)->segment;
        TOID (struct Directory) _dir;
        POBJ_ALLOC (pop.handle (), &_dir, struct Directory, sizeof (struct Directory), NULL, NULL);
        POBJ_ALLOC (pop.handle (), &D_RO (_dir)->segment, TOID (struct Segment),
                    sizeof (TOID (struct Segment)) * D_RO (dir)->capacity * 2, NULL, NULL);
        D_RW (_dir)->initDirectory (D_RO (dir)->depth + 1);

        for (int i = 0; i < D_RO (dir)->capacity; ++i) {
            if (i == x) {
                D_RW (D_RW (_dir)->segment)[2 * i] = s[0];
                D_RW (D_RW (_dir)->segment)[2 * i + 1] = s[1];
            } else {
                D_RW (D_RW (_dir)->segment)[2 * i] = D_RO (d)[i];
                D_RW (D_RW (_dir)->segment)[2 * i + 1] = D_RO (d)[i];
            }
        }

        pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (_dir)->segment)[0],
                         sizeof (TOID (struct Segment)) * D_RO (_dir)->capacity);
        pmemobj_persist (pop.handle (), (char *)&_dir, sizeof (struct Directory));
        dir = _dir;
        pmemobj_persist (pop.handle (), (char *)&dir, sizeof (TOID (struct Directory)));
#ifdef INPLACE
        D_RW (s[0])->local_depth++;
        pmemobj_persist (pop.handle (), (char *)&D_RO (s[0])->local_depth, sizeof (size_t));
        /* release segment exclusive lock */
        D_RW (s[0])->sema = 0;
#endif
        /* TBD */
        // POBJ_FREE(&dir_old);

    } else {  // normal split
        while (!D_RW (dir)->lock ()) {
            asm("nop");
        }
        x = (f_hash >> (8 * sizeof (f_hash) - D_RO (dir)->depth));
        if (D_RO (dir)->depth == D_RO (target)->local_depth + 1) {
            if (x % 2 == 0) {
                D_RW (D_RW (dir)->segment)[x + 1] = s[1];
#ifdef INPLACE
                pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[x + 1],
                                 sizeof (TOID (struct Segment)));
#else
                mfence ();
                D_RW (D_RW (dir)->segment)[x] = s[0];
                pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[x],
                                 sizeof (TOID (struct Segment)) * 2);
#endif
            } else {
                D_RW (D_RW (dir)->segment)[x] = s[1];
#ifdef INPLACE
                pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[x],
                                 sizeof (TOID (struct Segment)));
#else
                mfence ();
                D_RW (D_RW (dir)->segment)[x - 1] = s[0];
                pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[x - 1],
                                 sizeof (TOID (struct Segment)) * 2);
#endif
            }
            D_RW (dir)->unlock ();

#ifdef INPLACE
            D_RW (s[0])->local_depth++;
            pmemobj_persist (pop.handle (), (char *)&D_RO (s[0])->local_depth, sizeof (size_t));
            /* release target segment exclusive lock */
            D_RW (s[0])->sema = 0;
#endif
        } else {
            int stride = pow (2, D_RO (dir)->depth - target_local_depth);
            auto loc = x - (x % stride);
            for (int i = 0; i < stride / 2; ++i) {
                D_RW (D_RW (dir)->segment)[loc + stride / 2 + i] = s[1];
            }
#ifdef INPLACE
            pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[loc + stride / 2],
                             sizeof (TOID (struct Segment)) * stride / 2);
#else
            for (int i = 0; i < stride / 2; ++i) {
                D_RW (D_RW (dir)->segment)[loc + i] = s[0];
            }
            pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[loc],
                             sizeof (TOID (struct Segment)) * stride);
#endif
            D_RW (dir)->unlock ();
#ifdef INPLACE
            D_RW (s[0])->local_depth++;
            pmemobj_persist (pop.handle (), (char *)&D_RO (s[0])->local_depth, sizeof (size_t));
            /* release target segment exclusive lock */
            D_RW (s[0])->sema = 0;
#endif
        }
    }
    std::this_thread::yield ();
    goto RETRY;
}

void CCEH::mergeBufAndSplitWhenNeeded (pmem::obj::pool<CCEH> pop, WriteBuffer *bufnode,
                                       Segment_toid &target, size_t f_hash) {
    // bufnode has already been locked
    // out of place merge

    // step 1. copy old segment from pmem to dram
    Segment old_segment_dram;
    memcpy (&old_segment_dram, D_RW (target), sizeof (Segment));

    // step 2. migrate bufnode's kv to new_segment_dram
    auto iter = bufnode->Begin ();
    int inserted = 0;
    while (iter.Valid ()) {
        inserted++;
        auto &kv = *iter;
        Key_t key = kv.key;
        Value_t val = kv.val;
        auto f_hash = h (&key, sizeof (Key_t));
        if (!old_segment_dram.Insert4split (key, val, (f_hash & kMask) * kNumPairPerCacheLine)) {
            // insert fail, the new segment is full. we need to split it
            // INFO("Insert %d record, prepare to split", inserted);
            break;
        }
        ++iter;
    }

    if (iter.Valid ()) {  // we have to split new_segment_dram

        auto target_local_depth = D_RO (target)->local_depth;
        // INFO("Split segment %lu. depth %lu, ", x, target_local_depth);

        // step 1. Split the dram segment
        Segment *split = old_segment_dram.SplitDram (pop, iter);
        Segment *split_segment_dram = split;
        Segment *new_segment_dram = &old_segment_dram;
        new_segment_dram->local_depth = split_segment_dram->local_depth;
        new_segment_dram->bufnode_->local_depth = new_segment_dram->local_depth;

        // step 2. Copy dram version to pmem
        TOID (struct Segment) split_segment;
        POBJ_ALLOC (pop.handle (), &split_segment, struct Segment, sizeof (struct Segment), NULL,
                    NULL);
        // persist the split segment
        pmemobj_memcpy (pop.handle (), D_RW (split_segment), split_segment_dram,
                        sizeof (struct Segment), PMEMOBJ_F_MEM_NONTEMPORAL);
        TOID (struct Segment) new_segment;
        POBJ_ALLOC (pop.handle (), &new_segment, struct Segment, sizeof (struct Segment), NULL,
                    NULL);
        // persist the new segment after split
        pmemobj_memcpy (pop.handle (), D_RW (new_segment), new_segment_dram,
                        sizeof (struct Segment), PMEMOBJ_F_MEM_NONTEMPORAL);
        pmemobj_drain (pop.handle ());

        // step 3. Set the directory
    MERGE_SPLIT_RETRY:
        if (D_RO (target)->local_depth == D_RO (dir)->depth) {  // need double the directory
            if (!D_RW (dir)->suspend ()) {
                // INFO("Double directory conflicts");
                // other thread is doubling the directory
                std::this_thread::yield ();
                goto MERGE_SPLIT_RETRY;
            }
            printf ("Double Directory\n");
            INFO ("Double Directory Begin\n");

            // begin doubling
            TOID_ARRAY (TOID (struct Segment)) d = D_RO (dir)->segment;
            TOID (struct Directory) _dir;
            POBJ_ALLOC (pop.handle (), &_dir, struct Directory, sizeof (struct Directory), NULL,
                        NULL);
            POBJ_ALLOC (pop.handle (), &D_RO (_dir)->segment, TOID (struct Segment),
                        sizeof (TOID (struct Segment)) * D_RO (dir)->capacity * 2, NULL, NULL);
            D_RW (_dir)->initDirectory (D_RO (dir)->depth + 1);

            auto x = (f_hash >> (8 * sizeof (f_hash) - D_RO (dir)->depth));
            for (int i = 0; i < D_RO (dir)->capacity; ++i) {
                if (i == x) {
                    D_RW (D_RW (_dir)->segment)[2 * i] = new_segment;
                    D_RW (D_RW (_dir)->segment)[2 * i + 1] = split_segment;

                    DEBUG (
                        "Double directory segment %lu: 0x%lx. new segment 0x%lx to dir "
                        "%lu, depth: %lu, bufnode 0x%lx depth: %lu, split segment "
                        "0x%lx to dir %lu, depth: %lu, bufnode 0x%lx depth: %lu",
                        x, D_RW (target), D_RW (new_segment), 2 * i,
                        D_RW (new_segment)->local_depth, D_RW (new_segment)->bufnode_,
                        D_RW (new_segment)->bufnode_->local_depth, D_RW (split_segment), 2 * i + 1,
                        D_RW (split_segment)->local_depth, D_RW (split_segment)->bufnode_,
                        D_RW (split_segment)->bufnode_->local_depth);
                } else {
                    D_RW (D_RW (_dir)->segment)[2 * i] = D_RO (d)[i];
                    D_RW (D_RW (_dir)->segment)[2 * i + 1] = D_RO (d)[i];
                    DEBUG (
                        "Double directory segment %lu: 0x%lx. move segment 0x%lx to "
                        "dir %lu and dir %lu",
                        x, D_RW (target), D_RW (D_RW (d)[i]), 2 * i, 2 * i + 1);
                }
            }

            pmemobj_flush (pop.handle (), (char *)&D_RO (D_RO (_dir)->segment)[0],
                           sizeof (TOID (struct Segment)) * D_RO (_dir)->capacity);
            pmemobj_flush (pop.handle (), (char *)&_dir, sizeof (struct Directory));
            pmemobj_drain (pop.handle ());
            dir = _dir;
            pmemobj_persist (pop.handle (), (char *)&dir, sizeof (TOID (struct Directory)));
        } else {  // normal split
            while (!D_RW (dir)->lock ()) {
                asm("nop");
            }
            // Here other thread may change the dir by doubling
            // We need to recalculate x
            auto x = (f_hash >> (8 * sizeof (f_hash) - D_RO (dir)->depth));
            if (D_RO (dir)->depth == D_RO (target)->local_depth + 1) {
                if (x % 2 == 0) {
                    D_RW (D_RW (dir)->segment)[x + 1] = split_segment;
                    pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[x + 1],
                                     sizeof (TOID (struct Segment)));

                    mfence ();
                    D_RW (D_RW (dir)->segment)[x] = new_segment;
                    pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[x],
                                     sizeof (TOID (struct Segment)) * 2);

                    DEBUG (
                        "Gd: %lu. Normal split segment %lu: 0x%lx. depth: %lu. new "
                        "segment 0x%lx to dir %lu, depth: %lu, bufnode 0x%lx depth: "
                        "%lu, split segment 0x%lx to dir %lu depth: %lu, bufnode 0x%lx "
                        "depth: %lu",
                        D_RO (dir)->depth, x, D_RW (target), D_RW (target)->local_depth,
                        D_RW (new_segment), x, D_RW (new_segment)->local_depth,
                        D_RW (new_segment)->bufnode_, D_RW (new_segment)->bufnode_->local_depth,
                        D_RW (split_segment), x + 1, D_RW (split_segment)->local_depth,
                        D_RW (split_segment)->bufnode_,
                        D_RW (split_segment)->bufnode_->local_depth);
                } else {
                    D_RW (D_RW (dir)->segment)[x] = split_segment;
                    pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[x],
                                     sizeof (TOID (struct Segment)));

                    mfence ();

                    D_RW (D_RW (dir)->segment)[x - 1] = new_segment;
                    pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[x - 1],
                                     sizeof (TOID (struct Segment)) * 2);

                    DEBUG (
                        "Gd: %lu. Normal split segment %lu: 0x%lx. depth: %lu. new "
                        "segment 0x%lx to dir %lu, depth: %lu, bufnode 0x%lx depth: "
                        "%lu, split segment 0x%lx to dir %lu depth: %lu, bufnode 0x%lx "
                        "depth: %lu",
                        D_RO (dir)->depth, x, D_RW (target), D_RW (target)->local_depth,
                        D_RW (new_segment), x - 1, D_RW (new_segment)->local_depth,
                        D_RW (new_segment)->bufnode_, D_RW (new_segment)->bufnode_->local_depth,
                        D_RW (split_segment), x, D_RW (split_segment)->local_depth,
                        D_RW (split_segment)->bufnode_,
                        D_RW (split_segment)->bufnode_->local_depth);
                }
            } else {
                // there are multiple dir entries pointing to split segment
                int stride = pow (2, D_RO (dir)->depth - target_local_depth);
                // INFO("Split stride %d ", stride);
                auto loc = x - (x % stride);
                for (int i = 0; i < stride / 2; ++i) {
                    D_RW (D_RW (dir)->segment)[loc + stride / 2 + i] = split_segment;
                    DEBUG (
                        "Gd: %lu. Stride split segment %lu: 0x%lx. depth: %lu. Split "
                        "segment 0x%lx to dir %lu depth: %lu, bufnode 0x%lx depth: %lu",
                        D_RO (dir)->depth, x, D_RW (target), D_RW (target)->local_depth,
                        D_RW (split_segment), loc + stride / 2 + i,
                        D_RW (split_segment)->local_depth, D_RW (split_segment)->bufnode_,
                        D_RW (split_segment)->bufnode_->local_depth);
                }
                pmemobj_persist (pop.handle (),
                                 (char *)&D_RO (D_RO (dir)->segment)[loc + stride / 2],
                                 sizeof (TOID (struct Segment)) * stride / 2);

                for (int i = 0; i < stride / 2; ++i) {
                    D_RW (D_RW (dir)->segment)[loc + i] = new_segment;
                    DEBUG (
                        "Gd: %lu. Normal split segment %lu: 0x%lx. depth: %lu. new "
                        "segment 0x%lx to dir %lu, depth: %lu, bufnode 0x%lx depth: %lu",
                        D_RO (dir)->depth, x, D_RW (target), D_RW (target)->local_depth,
                        D_RW (new_segment), loc + i, D_RW (new_segment)->local_depth,
                        D_RW (new_segment)->bufnode_, D_RW (new_segment)->bufnode_->local_depth);
                }
                pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[loc],
                                 sizeof (TOID (struct Segment)) * stride);
            }
            DEBUG (
                "Gd: %lu. Normal split end. segment %lu: 0x%lx. depth: %lu. new "
                "segment 0x%lx, depth: %lu, bufnode 0x%lx depth: %lu, split "
                "segment 0x%lx, depth: %lu, bufnode 0x%lx depth: %lu",
                D_RO (dir)->depth, x, D_RW (target), D_RW (target)->local_depth, D_RW (new_segment),
                D_RW (new_segment)->local_depth, D_RW (new_segment)->bufnode_,
                D_RW (new_segment)->bufnode_->local_depth, D_RW (split_segment),
                D_RW (split_segment)->local_depth, D_RW (split_segment)->bufnode_,
                D_RW (split_segment)->bufnode_->local_depth);
            D_RW (dir)->unlock ();
        }

    } else {  // all records in bufnode has been merge to new_segment_dram

        TOID (struct Segment) new_segment;
        POBJ_ALLOC (pop.handle (), &new_segment, struct Segment, sizeof (struct Segment), NULL,
                    NULL);
        // persist the new segment
        pmemobj_memcpy (pop.handle (), D_RW (new_segment), &old_segment_dram, sizeof (Segment),
                        PMEMOBJ_F_MEM_NONTEMPORAL);
        // wait for directory lock. Replace old segment with new segment
        while (!D_RW (dir)->lock ()) {
            asm("nop");
        }

        auto x = (f_hash >> (8 * sizeof (f_hash) - D_RO (dir)->depth));

        DEBUG (
            "Merge segment. old segment %lu: 0x%lx. local d: %lu, bufnode 0x%lx, "
            "d: %lu new segment 0x%lx. local d: %lu, bufnode 0x%lx, d: %lu",
            x, D_RW (target), D_RW (target)->local_depth, D_RW (target)->bufnode_,
            D_RW (target)->bufnode_->local_depth, D_RW (new_segment),
            D_RW (new_segment)->local_depth, D_RW (new_segment)->bufnode_,
            D_RW (new_segment)->bufnode_->local_depth);

        if (D_RO (dir)->depth == D_RO (target)->local_depth) {
            // only need to update one dir entry
            // INFO("Update one directory %lu", x);
            D_RW (D_RW (dir)->segment)[x] = new_segment;
            pmemobj_persist (pop.handle (), (char *)&D_RW (D_RW (dir)->segment)[x],
                             sizeof (TOID (struct Segment)));
        } else if (D_RO (dir)->depth == D_RO (target)->local_depth + 1) {
            if (x % 2 == 0) {
                // INFO("Merge. Update two directory %lu, %lu. Segment %lu depth: %u,
                // dir depth: %lu", x, x+1, x, D_RO(target)->local_depth,
                // D_RO(dir)->depth);
                D_RW (D_RW (dir)->segment)[x + 1] = new_segment;
                pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[x + 1],
                                 sizeof (TOID (struct Segment)));
                mfence ();
                D_RW (D_RW (dir)->segment)[x] = new_segment;
                pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[x],
                                 sizeof (TOID (struct Segment)) * 2);
            } else {
                // INFO("Merge. Update two directory %lu, %lu. Segment %lu depth: %u,
                // dir depth: %lu", x, x-1, x, D_RO(target)->local_depth,
                // D_RO(dir)->depth);
                D_RW (D_RW (dir)->segment)[x] = new_segment;
                pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[x],
                                 sizeof (TOID (struct Segment)));
                mfence ();
                D_RW (D_RW (dir)->segment)[x - 1] = new_segment;
                pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[x - 1],
                                 sizeof (TOID (struct Segment)) * 2);
            }
        } else {
            // there are multiple dir entries pointing to this segment
            // INFO("Update multiple directory");
            int stride = pow (2, D_RO (dir)->depth - D_RO (target)->local_depth);
            auto loc = x - (x % stride);
            for (int i = 0; i < stride / 2; ++i) {
                auto new_x = loc + stride / 2 + i;
                // INFO("Update multiple directory %lu", new_x);
                D_RW (D_RW (dir)->segment)[new_x] = new_segment;
            }
            pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[loc + stride / 2],
                             sizeof (TOID (struct Segment)) * stride / 2);
        }

        D_RW (dir)->unlock ();
    }

    bufnode->Reset ();
}

bool CCEH::Delete (Key_t &key) { return false; }

Value_t CCEH::Get (Key_t &key) {
    auto f_hash = hash_funcs[0](&key, sizeof (Key_t), f_seed);
    auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;
RETRY:
    while (D_RO (dir)->sema < 0) {
        asm("nop");
    }
    auto x = (f_hash >> (8 * sizeof (f_hash) - D_RO (dir)->depth));
    auto target = D_RO (D_RO (dir)->segment)[x];
    if (!D_RO (target)) {
        std::this_thread::yield ();
        goto RETRY;
    }

    char *val;
    auto res = D_RW (target)->bufnode_->Get (key, val);
    if (res) {
        return Value_t (val);
    } else {
        return get (key);
    }
}

Value_t CCEH::get (Key_t &key) {
    auto f_hash = hash_funcs[0](&key, sizeof (Key_t), f_seed);
    auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;

RETRY:
    while (D_RO (dir)->sema < 0) {
        asm("nop");
    }

    auto x = (f_hash >> (8 * sizeof (f_hash) - D_RO (dir)->depth));
    auto target = D_RO (D_RO (dir)->segment)[x];

    if (!D_RO (target)) {
        std::this_thread::yield ();
        goto RETRY;
    }

#ifdef INPLACE
    /* acquire segment shared lock */
    if (!D_RW (target)->lock ()) {
        std::this_thread::yield ();
        goto RETRY;
    }
#endif

    auto target_check = (f_hash >> (8 * sizeof (f_hash) - D_RO (dir)->depth));
    if (D_RO (target) != D_RO (D_RO (D_RO (dir)->segment)[target_check])) {
        D_RW (target)->unlock ();
        std::this_thread::yield ();
        goto RETRY;
    }

    for (int i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto loc = (f_idx + i) % Segment::kNumSlot;
        if (D_RO (target)->bucket[loc].key == key) {
            Value_t v = D_RO (target)->bucket[loc].value;
#ifdef INPLACE
            /* key found, release segment shared lock */
            D_RW (target)->unlock ();
#endif
            return v;
        }
    }

    auto s_hash = hash_funcs[2](&key, sizeof (Key_t), s_seed);
    auto s_idx = (s_hash & kMask) * kNumPairPerCacheLine;
    for (int i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
        auto loc = (s_idx + i) % Segment::kNumSlot;
        if (D_RO (target)->bucket[loc].key == key) {
            Value_t v = D_RO (target)->bucket[loc].value;
#ifdef INPLACE
            D_RW (target)->unlock ();
#endif
            return v;
        }
    }

#ifdef INPLACE
    /* key not found, release segment shared lock */
    D_RW (target)->unlock ();
#endif
    return NONE;
}

void CCEH::Recovery (pmem::obj::pool<CCEH> pop, size_t nthreads, vector<uint64_t> &v) {
    // ensure the consistent of the directory entries
    DEBUG ("Start Recovery CCEH ..");
    size_t i = 0;
    v.push_back (i);
    auto dirptr = D_RO (dir);

    while (i < dirptr->capacity) {
        size_t depth_cur = D_RO (D_RO (D_RO (dir)->segment)[i])->local_depth;
        size_t stride = pow (2, D_RO (dir)->depth - depth_cur);
        size_t buddy = i + stride;
        if (buddy == D_RO (dir)->capacity) break;
        for (size_t j = buddy - 1; i < j; j--) {
            if (D_RO (D_RO (D_RO (dir)->segment)[j])->local_depth != depth_cur) {
                D_RW (D_RW (dir)->segment)[j] = D_RO (D_RO (dir)->segment)[i];
                pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[i],
                                 sizeof (TOID (struct Segment)));
            }
        }

        i += stride;
        v.push_back (i);
    }
    pmemobj_persist (pop.handle (), (char *)&D_RO (D_RO (dir)->segment)[0],
                     sizeof (TOID (struct Segment)) * D_RO (dir)->capacity);
    printf ("size of segidx = %lu \n", v.size ());
}

void CCEH::BufferRecovery1 (pmem::obj::pool<CCEH> pop, uint64_t nthread) {
    // ensure the consistent of the directory entries
    DEBUG ("Start Recovery Buffer ..");
    size_t cntRecovery = 0;
    auto dirptr = D_RO (dir);
    for (auto l = 0; l < nthread; l++) {
        pop.root ()->BufferLogNodes[l].Open ();
    }

    size_t i = 0;

    while (i < dirptr->capacity) {
        size_t depth_cur = D_RO (D_RO (D_RO (dir)->segment)[i])->local_depth;
        size_t stride = pow (2, D_RO (dir)->depth - depth_cur);
        size_t buddy = i + stride;
        if (buddy == D_RO (dir)->capacity) break;

        auto ptr = D_RW (D_RW (D_RW (dir)->segment)[i]);
        uint64_t logid = ptr->logPtr->getLogId ();
        uint64_t offset = ptr->logPtr->getOffset ();

        // printf ("data %lu id %lu offset %lu \n", ptr->logPtr->getData (), logid, offset);

        ptr->bufnode_ = new WriteBuffer (ptr->local_depth, 32 + (1 + 0.3));
        if (pop.root ()->BufferLogNodes[logid].Valid (offset)) {
            uint64_t nid = logid;
            uint64_t noffset = offset;
            uint64_t d = 0;
            do {
                char *currentAddr = pop.root ()->BufferLogNodes[nid].currentAddr (noffset);
                uint64_t key = *(uint64_t *)(currentAddr - 16);
                char *val = (char *)(currentAddr - 8);
                ptr->bufnode_->Put (key, val);

                uint64_t data = *(uint64_t *)(currentAddr + 2);
                d = data;
                nid = data >> 48;
                noffset = data & 0x0000FFFFFFFFFFFF;

                cntRecovery++;

            } while (pop.root ()->BufferLogNodes[nid].IsValidPoint (noffset) && (d != 0));
        }

        i += stride;
    }
    printf ("Finished Recovery (total = %lu) \n", cntRecovery);
}

inline uint64_t NowMicros () {
    static constexpr uint64_t kUsecondsPerSecond = 1000000;
    struct ::timeval tv;
    ::gettimeofday (&tv, nullptr);
    return static_cast<uint64_t> (tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
}

void CCEH::BufferRecovery2 (pmem::obj::pool<CCEH> pop, uint64_t nthread, int tid) {
    uint64_t start1_ = NowMicros ();
    // ensure the consistent of the directory entries
    DEBUG ("Start Recovery Buffer ..");
    uint64_t cntRecovery = 0;

    pop.root ()->BufferLogNodes[tid].Open ();

    char *start = pop.root ()->BufferLogNodes[tid].log.get () + 256 + 17;
    uint64_t test = 256 + 17;
    while (1) {
        uint64_t key = *(uint64_t *)(start - 16);
        uint64_t val = *(uint64_t *)(start - 8);
        uint8_t tag = *(uint8_t *)(start);
        // printf ("tag = %hhu \n", tag);
        if (tag != 16 && tag != 18) break;

        auto f_hash = hash_funcs[0](&key, sizeof (Key_t), f_seed);
        auto x = (f_hash >> (8 * sizeof (f_hash) - D_RO (dir)->depth));
        auto target = D_RO (D_RO (dir)->segment)[x];
        auto target_ptr = D_RW (target);
        if (!target_ptr->buf_flag) {
            target_ptr->bufnode_ = new WriteBuffer (target_ptr->local_depth, 32 * (1 + 0.3));
            target_ptr->buf_flag = true;
        }
        target_ptr->bufnode_->PutWithLog (key, (char *)val);

        start = start + 27;
        test += 27;
        cntRecovery++;
    }

    // ptr->bufnode_ = new WriteBuffer (ptr->local_depth, 32 + (1 + 0.3));

    uint64_t end1_ = NowMicros ();
    printf ("Finished Recovery in %2.10f secs (total = %lu) \n", (end1_ - start1_) / 1000000.0,
            cntRecovery);
}

double CCEH::Utilization (void) {
    size_t sum = 0;
    size_t cnt = 0;
    for (int i = 0; i < D_RO (dir)->capacity; ++cnt) {
        auto target = D_RO (D_RO (dir)->segment)[i];
        int stride = pow (2, D_RO (dir)->depth - D_RO (target)->local_depth);
        auto pattern = (i >> (D_RO (dir)->depth - D_RO (target)->local_depth));
        for (unsigned j = 0; j < Segment::kNumSlot; ++j) {
            auto f_hash = h (&D_RO (target)->bucket[j].key, sizeof (Key_t));
            if (((f_hash >> (8 * sizeof (f_hash) - D_RO (target)->local_depth)) == pattern) &&
                (D_RO (target)->bucket[j].key != INVALID)) {
                sum++;
            }
        }
        i += stride;
    }
    return ((double)sum) / ((double)cnt * Segment::kNumSlot) * 100.0;
}

size_t CCEH::Capacity (void) {
    size_t cnt = 0;
    for (int i = 0; i < D_RO (dir)->capacity; cnt++) {
        auto target = D_RO (D_RO (dir)->segment)[i];
        int stride = pow (2, D_RO (dir)->depth - D_RO (target)->local_depth);
        i += stride;
    }

    return cnt * Segment::kNumSlot;
}

// for debugging
Value_t CCEH::FindAnyway (Key_t &key) {
    for (size_t i = 0; i < D_RO (dir)->capacity; ++i) {
        for (size_t j = 0; j < Segment::kNumSlot; ++j) {
            if (D_RO (D_RO (D_RO (dir)->segment)[i])->bucket[j].key == key) {
                cout << "segment(" << i << ")" << endl;
                cout << "global_depth(" << D_RO (dir)->depth << "), local_depth("
                     << D_RO (D_RO (D_RO (dir)->segment)[i])->local_depth << ")" << endl;
                cout << "pattern: "
                     << bitset<sizeof (int64_t)> (
                            i >>
                            (D_RO (dir)->depth - D_RO (D_RO (D_RO (dir)->segment)[i])->local_depth))
                     << endl;
                cout << "Key MSB: "
                     << bitset<sizeof (int64_t)> (
                            h (&key, sizeof (key)) >>
                            (8 * sizeof (key) - D_RO (D_RO (D_RO (dir)->segment)[i])->local_depth))
                     << endl;
                return D_RO (D_RO (D_RO (dir)->segment)[i])->bucket[j].value;
            }
        }
    }
    return NONE;
}

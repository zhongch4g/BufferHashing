#include <iostream>
#include <thread>
#include <bitset>
#include <cassert>
#include <unordered_map>
#include <stdio.h>
#include <vector>
#include "CCEH_BUF.h"
#include "hash.h"
#include "util.h"
#include "logger.h"

#define f_seed 0xc70697UL
#define s_seed 0xc70697UL
//#define f_seed 0xc70f6907UL
//#define s_seed 0xc70f6907UL

#define INPLACE
// #define WITHOUT_FLUSH
#define CONFIG_OUT_OF_PLACE_MERGE

using namespace std;

void Segment::execute_path(PMEMobjpool* pop, vector<pair<size_t, size_t>>& path, Key_t& key, Value_t value){
    for(int i=path.size()-1; i>0; --i){
	bucket[path[i].first] = bucket[path[i-1].first];
	pmemobj_persist(pop, (char*)&bucket[path[i].first], sizeof(Pair));
    }
    bucket[path[0].first].value = value;
    mfence();
    bucket[path[0].first].key = key;
    pmemobj_persist(pop, (char*)&bucket[path[0].first], sizeof(Pair));
}

void Segment::execute_path(vector<pair<size_t, size_t>>& path, Pair _bucket){
    int i = 0;
    int j = (i+1) % 2;

    Pair temp[2];
    temp[0] = _bucket;
    for(auto p: path){
	temp[j] = bucket[p.first];
	bucket[p.first] = temp[i];
	i = (i+1) % 2;
	j = (i+1) % 2;
    }
}
	
vector<pair<size_t, size_t>> Segment::find_path(size_t target, size_t pattern){
    vector<pair<size_t, size_t>> path;
    path.reserve (kCuckooThreshold);
    path.emplace_back (target, bucket[target].key);

    auto cur = target;
    int i = 0;

    do {
	Key_t* key = &bucket[cur].key;
	auto f_hash = hash_funcs[0](key, sizeof(Key_t), f_seed);
	auto s_hash = hash_funcs[2](key, sizeof(Key_t), s_seed);

	if((f_hash >> (8*sizeof(f_hash) - local_depth)) != pattern || *key == INVALID){
	    break;
	}

	for(int j=0; j<kNumPairPerCacheLine*kNumCacheLine; ++j){
	    auto f_idx = (((f_hash & kMask) * kNumPairPerCacheLine) + j) % kNumSlot;
	    auto s_idx = (((s_hash & kMask) * kNumPairPerCacheLine) + j) % kNumSlot;

	    if(f_idx == cur) {
			path.emplace_back(s_idx, bucket[s_idx].key);
			cur = s_idx;
			break;
	    }
	    else if(s_idx == cur) {
			path.emplace_back(f_idx, bucket[f_idx].key);
			cur = f_idx;
			break;
	    }
	}
	++i;
    } while (i < kCuckooThreshold);

    if (i == kCuckooThreshold){
		path.resize(0);
    }
	// ?
    // return move(path);
	return (path);
}


bool Segment::Insert4split(Key_t& key, Value_t value, size_t loc) {
	// Linear probing : search kNumCacheLine Buckets
    for(int i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
		auto slot = (loc + i) % kNumSlot;
		if(bucket[slot].key == INVALID) {
			bucket[slot].key = key;
			bucket[slot].value = value;
			return 1;
		}
    }
    return 0;
}

// TODO:Segment in dram Split
Segment *Segment::SplitDram(WriteBuffer::Iterator &iter) {
	Segment *split = new Segment();
	split->initSegment(local_depth + 1);
	// printf("Split Dram \n");
	// local depth increase 1 after split! Move the KV to new segment according to pattern
	auto pattern = ((size_t)1 << (sizeof(Key_t) * 8 - local_depth - 1));

	// traverse all the kv pairs in Segment
	for (int i = 0; i < kNumSlot; ++i) {
		auto f_hash = h(&bucket[i].key, sizeof(Key_t));
		if (f_hash & pattern) {
			if (!split->Insert4split(bucket[i].key, bucket[i].value,
									(f_hash & kMask) * kNumPairPerCacheLine)) {
				// if can not insert, try another hash function
				auto s_hash = hash_funcs[2](&bucket[i].key, sizeof(Key_t), s_seed);
				if (!split->Insert4split(bucket[i].key, bucket[i].value,
										(s_hash & kMask) * kNumPairPerCacheLine)) {
					// insert fail
					INFO("Hash 1 insert split segment fail ");
				}
			}
			// invalidate the migrated key
			bucket[i].key = INVALID;
		}
	}

	/* Continue to moving data from buffer to splitted segment */
	while (iter.Valid()) {
		auto &kv = *iter;
		Key_t key = kv.key;
		Value_t val = kv.val;
		auto f_hash = h(&key, sizeof(Key_t));
		if (f_hash & pattern) {
			// insert to split segment
			if (!split->Insert4split(key, val,
									(f_hash & kMask) * kNumPairPerCacheLine)) {
				// try another hash function
				auto s_hash = hash_funcs[2](&key, sizeof(Key_t), s_seed);
				if (!split->Insert4split(key, val,
										(s_hash & kMask) * kNumPairPerCacheLine)) {
					// insert to split segment fail
					// insert to the buffer of split segment
					// LOG required
					printf("second split \n");
					split->bufnode_->Lock();
					bool isValidPut = split->bufnode_->Put(key, (char*)val);
					split->bufnode_->Unlock();
				}
			}
		} else {
			if (!Insert4split(key, val, (f_hash & kMask) * kNumPairPerCacheLine)) {
				// try another hash function
				auto s_hash = hash_funcs[2](&key, sizeof(Key_t), s_seed);
				if (!Insert4split(key, val, (s_hash & kMask) * kNumPairPerCacheLine)) {
					// insert to original segment fail
					// insert to the buffer of split segment
					// LOG required
					INFO("");
				}
			}
		}
		++iter;
	}
	return split;
}

Value_t CCEH::Get(Key_t &key) {
	/* Key hashing */
	auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
	/* kMask: 1111 1111 */
	auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;
RETRY:
	/* check validation */
	while (D_RO(dir)->sema < 0) {
		asm("nop");
	}

	auto x = (f_hash >> (8 * sizeof(f_hash) - D_RO(dir)->depth));
  	auto target = D_RO(D_RO(dir)->segment)[x];
	if (!D_RO(target)) {
		std::this_thread::yield();
		goto RETRY;
	}

	char *val;
	auto res = D_RW(target)->bufnode_->Get(key, val);
	// printf("Valid Get = %d , key = %d, val = %s \n", res, key, val);
	if (res) {
		return Value_t(val);
	} else {
		return get(key);
	}
}

Value_t CCEH::get(Key_t &key) {
	auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
	auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;

RETRY:
	while (D_RO(dir)->sema < 0) {
		asm("nop");
		}

	auto x = (f_hash >> (8 * sizeof(f_hash) - D_RO(dir)->depth));
	auto target = D_RO(D_RO(dir)->segment)[x];
	
	if (!D_RO(target)) {
		std::this_thread::yield();
    	goto RETRY;
  	}

#ifdef INPLACE
	/* acquire segment shared lock */
	if (!D_RW(target)->lock()) {
		std::this_thread::yield();
		goto RETRY;
	}
#endif

	auto target_check = (f_hash >> (8 * sizeof(f_hash) - D_RO(dir)->depth));
	if (D_RO(target) != D_RO(D_RO(D_RO(dir)->segment)[target_check])) {
		D_RW(target)->unlock();
		std::this_thread::yield();
		goto RETRY;
	}

	for (int i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
		auto loc = (f_idx + i) % Segment::kNumSlot;
		if (D_RO(target)->bucket[loc].key == key) {
			Value_t v = D_RO(target)->bucket[loc].value;
	#ifdef INPLACE
			/* key found, release segment shared lock */
			D_RW(target)->unlock();
	#endif
			return v;
		}
	}

	auto s_hash = hash_funcs[2](&key, sizeof(Key_t), s_seed);
	auto s_idx = (s_hash & kMask) * kNumPairPerCacheLine;
	for (int i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
		auto loc = (s_idx + i) % Segment::kNumSlot;
		if (D_RO(target)->bucket[loc].key == key) {
			Value_t v = D_RO(target)->bucket[loc].value;
#ifdef INPLACE
      		D_RW(target)->unlock();
#endif
      		return v;
		}
	}

#ifdef INPLACE
	/* key not found, release segment shared lock */
	D_RW(target)->unlock();
#endif
  	return NONE;

}

TOID(struct Segment)* Segment::Split(PMEMobjpool* pop){
#ifdef INPLACE
	/* Create another two Segment */
    TOID(struct Segment)* split = new TOID(struct Segment)[2];

    split[0] = pmemobj_oid(this);

    POBJ_ALLOC(pop, &split[1], struct Segment, sizeof(struct Segment), NULL, NULL);
    D_RW(split[1])->initSegment(local_depth + 1);

    auto pattern = ((size_t)1 << (sizeof(Key_t) * 8 - local_depth - 1));

    for(int i=0; i<kNumSlot; ++i){
		auto f_hash = hash_funcs[0](&bucket[i].key, sizeof(Key_t), f_seed);
		if(f_hash & pattern){
			if(!D_RW(split[1])->Insert4split(bucket[i].key, bucket[i].value, (f_hash & kMask)*kNumPairPerCacheLine)) {
				auto s_hash = hash_funcs[2](&bucket[i].key, sizeof(Key_t), s_seed);
				if(!D_RW(split[1])->Insert4split(bucket[i].key, bucket[i].value, (s_hash & kMask)*kNumPairPerCacheLine)) {
				
				}
	    	}
		}
    }

    pmemobj_persist(pop, (char*)D_RO(split[1]), sizeof(struct Segment));
    return split;
#else
	/* Create another two Segment */
    TOID(struct Segment)* split = new TOID(struct Segment)[2];
    POBJ_ALLOC(pop, &split[0], struct Segment, sizeof(struct Segment), NULL, NULL);
    POBJ_ALLOC(pop, &split[1], struct Segment, sizeof(struct Segment), NULL, NULL);
    D_RW(split[0])->initSegment(local_depth + 1);
    D_RW(split[1])->initSegment(local_depth + 1);

    auto pattern = ((size_t)1 << (sizeof(Key_t)*8 - local_depth - 1));
    for(int i = 0; i <  kNumSlot; ++i) {
		auto f_hash = h(&bucket[i].key, sizeof(Key_t));
		if(f_hash & pattern){
			D_RW(split[1])->Insert4split(bucket[i].key, bucket[i].value, (f_hash & kMask)*kNumPairPerCacheLine);
		} else {
			D_RW(split[0])->Insert4split(bucket[i].key, bucket[i].value, (f_hash & kMask)*kNumPairPerCacheLine);
		}
    }

    pmemobj_persist(pop, (char*)D_RO(split[0]), sizeof(struct Segment));
    pmemobj_persist(pop, (char*)D_RO(split[1]), sizeof(struct Segment));

    return split;
#endif
}

void CCEH::initCCEH(PMEMobjpool* pop){
    crashed = true;
	// allocate space for directory
    POBJ_ALLOC(pop, &dir, struct Directory, sizeof(struct Directory), NULL, NULL);
    D_RW(dir)->initDirectory();
	// allocate space for segment
    POBJ_ALLOC(pop, &D_RW(dir)->segment, TOID(struct Segment), sizeof(TOID(struct Segment))*D_RO(dir)->capacity, NULL, NULL);

    for(int i=0; i<D_RO(dir)->capacity; ++i){
	// allocate space for all segment
	POBJ_ALLOC(pop, &D_RO(D_RO(dir)->segment)[i], struct Segment, sizeof(struct Segment), NULL, NULL);
	D_RW(D_RW(D_RW(dir)->segment)[i])->initSegment();
    }
}

void CCEH::initCCEH(PMEMobjpool* pop, size_t initCap){
    crashed = true;
    POBJ_ALLOC(pop, &dir, struct Directory, sizeof(struct Directory), NULL, NULL);
    D_RW(dir)->initDirectory(static_cast<size_t>(log2(initCap)));
    POBJ_ALLOC(pop, &D_RW(dir)->segment, TOID(struct Segment), sizeof(TOID(struct Segment))*D_RO(dir)->capacity, NULL, NULL);

    for(int i=0; i<D_RO(dir)->capacity; ++i) {
		POBJ_ALLOC(pop, &D_RO(D_RO(dir)->segment)[i], struct Segment, sizeof(struct Segment), NULL, NULL);
		D_RW(D_RW(D_RW(dir)->segment)[i])->initSegment(static_cast<size_t>(log2(initCap)));
    }
}

void CCEH::Insert(PMEMobjpool* pop, Key_t& key, Value_t value) {
	/* Key hashing */
	auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
    // auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;
retry:
	/* MSB ex. (dir-depth-length-leading-bit) 11111000... Get the position of the target segment */
	auto x = (f_hash >> (8 * sizeof(f_hash) - D_RO(dir)->depth));
	/* the target segment (Read only) */
    auto target = D_RO(D_RO(dir)->segment)[x];
	/* write the target segment */
	auto target_ptr = D_RW(target);

	/* add lock when buffer insert */
	target_ptr->bufnode_->Lock();

	/* Guaranteed consistency 
	 * Access the target segment when this segment is spliting by other thread will cause the difference of the depth. 
	 */
	if (D_RO(target)->local_depth != target_ptr->bufnode_->local_depth) {
		/* release the lock */
		target_ptr->bufnode_->Unlock();
		std::this_thread::yield();
		/* the use of goto statement in multithread */
		goto retry;
	}

	bool isValidPut = target_ptr->bufnode_->Put(key, (char*)value);
	/* put the KV to the buffer */
	if (isValidPut) {
		target_ptr->bufnode_->Unlock();
	} else {
		/* the buffer is full, merge the buffer to the segment (OUT-OF-PLACE Merge) */
#ifndef CONFIG_OUT_OF_PLACE_MERGE
		auto iter = D_RW(target)->bufnode_->Begin();
		while (iter.Valid()) {
			auto &kv = *iter;
			Key_t key = kv.key;
			Value_t val = kv.val;
			insert(pop, key, val, false);
			++iter;
		}
#ifdef WITHOUT_FLUSH
		pmemobj_persist(pop, (char *)&D_RO(D_RO(dir)->segment)[x],
						sizeof(TOID(struct Segment)));
#endif
		D_RW(target)->bufnode_->Reset();
		D_RW(target)->bufnode_->Unlock();
		goto retry;
#else
		mergeBufAndSplitWhenNeeded(pop, target_ptr->bufnode_, target, f_hash);
		target_ptr->bufnode_->Unlock();
		goto retry;
#endif
	}

	/* 4K Buffer 256B for each buffer node -> 16 buffer node for each buffer */
	/* 4K Buffer 256B for each buffer node -> 16 buffer node for each buffer */
}
 
void CCEH::insert(PMEMobjpool* pop, Key_t& key, Value_t value, bool with_lock){

    auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
	// LSB
    auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;

RETRY:
	/* MSB ex. (dir-depth-length-leading-bit) 11111000... Get the position of the target segment */
    auto x = (f_hash >> (8 * sizeof(f_hash) - D_RO(dir)->depth));
    auto target = D_RO(D_RO(dir)->segment)[x]; // the target segment

	// char val[128] = "value123";
	/* check the buffer has enough space or not (Inside the Put func)*/
	// bool valid_Put = nodes_[x].Put((int64_t)key, (char*)value);
	// bool isValidPut = writeBuffer_[x].Put((int64_t)key, (char*)value);
	

	/*
	uint64_t segFullCheck = 1;
	for (size_t j = 0; j < 13; j++) {
		// printf("%lu out of 13 : Key %lu, value %s \n", j, nodes_[x].kvs_[j].key, nodes_[x].kvs_[j].val);
		if (nodes_[x].kvs_[j].val)
			segFullCheck++;
	}
	printf("%lu Segment space usage %lu//13 \n", x, segFullCheck);
	if (segFullCheck == 12)
		printf("Segment full, current index = %lu \n", key);
	*/

    if(!D_RO(target)) {
		std::this_thread::yield();
		goto RETRY;
    }
    
	if (with_lock) {
		/* acquire segment exclusive lock */
		if(!D_RW(target)->lock()) {
			std::this_thread::yield();
			goto RETRY;
		}
	}

	/* recheck after get the lock? */
    auto target_check = (f_hash >> (8 * sizeof(f_hash) - D_RO(dir)->depth)); 
    if(D_RO(target) != D_RO(D_RO(D_RO(dir)->segment)[target_check])) {
		if (with_lock) 
			D_RW(target)->unlock();
		std::this_thread::yield();
		goto RETRY;
    }

	/* local depth pattern */ 
    auto pattern = (f_hash >> (8 * sizeof(f_hash) - D_RO(target)->local_depth));

	/* Linear Probing : key insertion 1 */
    for(unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i){
		auto loc = (f_idx + i) % Segment::kNumSlot;
		auto _key = D_RO(target)->bucket[loc].key;
		/* validity check for entry keys */
		if((
			((hash_funcs[0](&D_RO(target)->bucket[loc].key, sizeof(Key_t), f_seed) >> (8*sizeof(f_hash)-D_RO(target)->local_depth)) != pattern)
			|| (D_RO(target)->bucket[loc].key == INVALID)
			) 
			&& (D_RO(target)->bucket[loc].key != SENTINEL)){
			if(CAS(&D_RW(target)->bucket[loc].key, &_key, SENTINEL)){
				// value first
				D_RW(target)->bucket[loc].value = value;
	#ifndef WITHOUT_FLUSH
				// add mfence to ensure the order of the insertion
				mfence();
	#endif
				// key later
				D_RW(target)->bucket[loc].key = key;
	#ifndef WITHOUT_FLUSH
				pmemobj_persist(pop, (char*)&D_RO(target)->bucket[loc], sizeof(Pair));
	#endif
				/* release segment exclusive lock */
				if (with_lock) 
					D_RW(target)->unlock();
				return;
			}
		}
    }

	

	/* use another hash function to find the slot if prev step fail insert : key insertion 2*/
    auto s_hash = hash_funcs[2](&key, sizeof(Key_t), s_seed);
    auto s_idx = (s_hash & kMask) * kNumPairPerCacheLine;

    for(unsigned i=0; i<kNumPairPerCacheLine * kNumCacheLine; ++i){
		auto loc = (s_idx + i) % Segment::kNumSlot;
		auto _key = D_RO(target)->bucket[loc].key;
		if((
			((hash_funcs[0](&D_RO(target)->bucket[loc].key, sizeof(Key_t), f_seed) >> (8*sizeof(s_hash)-D_RO(target)->local_depth)) != pattern) 
			|| (D_RO(target)->bucket[loc].key == INVALID)
			) 
			&& (D_RO(target)->bucket[loc].key != SENTINEL)) {

				if(CAS(&D_RW(target)->bucket[loc].key, &_key, SENTINEL)){
				D_RW(target)->bucket[loc].value = value;
#ifndef WITHOUT_FLUSH
				mfence();
#endif
				D_RW(target)->bucket[loc].key = key;
#ifndef WITHOUT_FLUSH
				pmemobj_persist(pop, (char*)&D_RO(target)->bucket[loc], sizeof(Pair));
#endif
				if (with_lock)
					D_RW(target)->unlock();
				return;
				}
			}
    }

    auto target_local_depth = D_RO(target)->local_depth;
    // COLLISION !!
    /* need to split segment but release the exclusive lock first to avoid deadlock */
	if (with_lock)
    	D_RW(target)->unlock();

	// suspend?
    if(with_lock && !D_RW(target)->suspend()) {
	std::this_thread::yield();
	goto RETRY;
    }

    /* need to check whether the target segment has been split */
#ifdef INPLACE
    if(target_local_depth != D_RO(target)->local_depth) {
		D_RW(target)->sema = 0;
		std::this_thread::yield();
		goto RETRY;
    }
#else
    if(target_local_depth != D_RO(D_RO(D_RO(dir)->segment)[x])->local_depth) {
		D_RW(target)->sema = 0;
		std::this_thread::yield();
		goto RETRY;
    }
#endif

    TOID(struct Segment)* s = D_RW(target)->Split(pop);
DIR_RETRY:
    /* need to double the directory */
    if (D_RO(target)->local_depth == D_RO(dir)->depth) {
		if (!D_RW(dir)->suspend()) {
			std::this_thread::yield();
			goto DIR_RETRY;
		}

		x = (f_hash >> (8*sizeof(f_hash) - D_RO(dir)->depth));
		auto dir_old = dir;
		TOID_ARRAY(TOID(struct Segment)) d = D_RO(dir)->segment;
		TOID(struct Directory) _dir;
		POBJ_ALLOC(pop, &_dir, struct Directory, sizeof(struct Directory), NULL, NULL);
		POBJ_ALLOC(pop, &D_RO(_dir)->segment, TOID(struct Segment), sizeof(TOID(struct Segment))*D_RO(dir)->capacity*2, NULL, NULL);
		D_RW(_dir)->initDirectory(D_RO(dir)->depth+1);

		for(int i = 0; i < D_RO(dir)->capacity; ++i){
			if(i == x) {
				D_RW(D_RW(_dir)->segment)[2 * i] = s[0];
				D_RW(D_RW(_dir)->segment)[2 * i + 1] = s[1];
			}
			else {
				D_RW(D_RW(_dir)->segment)[2 * i] = D_RO(d)[i];
				D_RW(D_RW(_dir)->segment)[2 * i + 1] = D_RO(d)[i];
			}
		}

		pmemobj_persist(pop, (char*)&D_RO(D_RO(_dir)->segment)[0], sizeof(TOID(struct Segment))*D_RO(_dir)->capacity);
		pmemobj_persist(pop, (char*)&_dir, sizeof(struct Directory));
		dir = _dir;
		pmemobj_persist(pop, (char*)&dir, sizeof(TOID(struct Directory)));
		
#ifdef INPLACE
		D_RW(s[0])->local_depth++;
		pmemobj_persist(pop, (char*)&D_RO(s[0])->local_depth, sizeof(size_t));
		/* release segment exclusive lock */
		D_RW(s[0])->sema = 0;
#endif
		/* TBD */
		// POBJ_FREE(&dir_old);

    } else { // normal split
		while(!D_RW(dir)->lock()) {
			asm("nop");
		}
		x = (f_hash >> (8 * sizeof(f_hash) - D_RO(dir)->depth));
		if(D_RO(dir)->depth == D_RO(target)->local_depth + 1){
			if(x % 2 == 0) {
				D_RW(D_RW(dir)->segment)[x+1] = s[1];
#ifdef INPLACE
				pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x+1], sizeof(TOID(struct Segment)));
#else
				mfence();
				D_RW(D_RW(dir)->segment)[x] = s[0];
				pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x], sizeof(TOID(struct Segment))*2);
#endif
			} else {
				D_RW(D_RW(dir)->segment)[x] = s[1];
#ifdef INPLACE
				pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x], sizeof(TOID(struct Segment)));
#else
				mfence();
				D_RW(D_RW(dir)->segment)[x - 1] = s[0];
				pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x - 1], sizeof(TOID(struct Segment)) * 2);
#endif
			}
			D_RW(dir)->unlock();

#ifdef INPLACE
			D_RW(s[0])->local_depth++;
			pmemobj_persist(pop, (char*)&D_RO(s[0])->local_depth, sizeof(size_t));
			/* release target segment exclusive lock */
			D_RW(s[0])->sema = 0;
#endif
		} else {
			int stride = pow(2, D_RO(dir)->depth - target_local_depth);
			auto loc = x - (x % stride);
			for(int i=0; i<stride/2; ++i) {
				D_RW(D_RW(dir)->segment)[loc+stride/2+i] = s[1];
			}
#ifdef INPLACE
			pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[loc+stride/2], sizeof(TOID(struct Segment))*stride/2);
#else
			for(int i = 0; i < stride / 2; ++i) {
				D_RW(D_RW(dir)->segment)[loc+i] = s[0];
			}
			pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[loc], sizeof(TOID(struct Segment))*stride);
#endif
			D_RW(dir)->unlock();
#ifdef INPLACE
			D_RW(s[0])->local_depth++;
			pmemobj_persist(pop, (char*)&D_RO(s[0])->local_depth, sizeof(size_t));
			/* release target segment exclusive lock */
			D_RW(s[0])->sema = 0;
	#endif
		}
    }
    std::this_thread::yield();
    goto RETRY;
}

/* OUT-OF-PLACE Merge */
void CCEH::mergeBufAndSplitWhenNeeded(PMEMobjpool* pop, WriteBuffer* bufnode, Segment_toid& target, size_t f_hash) {
	/* current segment buffer node already locked */

	/* Step 1. copy old segment from pmem to dram */
	Segment old_segment_dram;
	// old_segment_dram = D_RW(target);
	memcpy(&old_segment_dram, D_RW(target), sizeof(Segment));
	/* Step 2. migrate bufnode's kv to new_segment_dram */
	auto iter = bufnode->Begin();
	while (iter.Valid()) {
		auto& kv = *iter;
		Key_t key = kv.key;
		Value_t val = kv.val;
		// h hash
		auto f_hash = h(&key, sizeof(Key_t));
		// locate the bucket (f_hash & kMask) * kNumPairPerCacheLine
		if (!old_segment_dram.Insert4split(key, val, (f_hash & kMask) * kNumPairPerCacheLine)) {
			break;
		}
		++iter;
	}

	/* there is no place to insert*/
	if (iter.Valid()) {
		auto target_local_depth = D_RO(target)->local_depth;
		/* step 1. split the segment in dram */
		Segment* split = old_segment_dram.SplitDram(iter);
		Segment* split_segment_dram = split;
		Segment* new_segment_dram = &old_segment_dram;
		new_segment_dram->local_depth = split_segment_dram->local_depth;
		new_segment_dram->bufnode_->local_depth = new_segment_dram->local_depth;

		/* step 2. Copy dram version to pmem */
		TOID(struct Segment) split_segment;
		POBJ_ALLOC(pop, &split_segment, struct Segment, sizeof(struct Segment), NULL, NULL);
		/* persist the split segment */
		pmemobj_memcpy(pop, D_RW(split_segment), split_segment_dram, sizeof(struct Segment), PMEMOBJ_F_MEM_NONTEMPORAL);
		
		TOID(struct Segment) new_segment;
		POBJ_ALLOC(pop, &new_segment, struct Segment, sizeof(struct Segment), NULL, NULL);
		/* persist the new segment after split */
		pmemobj_memcpy(pop, D_RW(new_segment), new_segment_dram, sizeof(struct Segment), PMEMOBJ_F_MEM_NONTEMPORAL);
		pmemobj_drain(pop);

		/* step 3. Set the directory */
		MERGE_SPLIT_RETRY:
			if (D_RO(target)->local_depth == D_RO(dir)->depth) { // double the directory
				if (!D_RW(dir)->suspend()) {
					/* other thread is doubling the directory */
					std::this_thread::yield();
					goto MERGE_SPLIT_RETRY;
				}
				// printf("Double the directory \n");
				
				TOID_ARRAY(TOID(struct Segment)) d = D_RO(dir)->segment;
				TOID(struct Directory) _dir;
				POBJ_ALLOC(pop, &_dir, struct Directory, sizeof(struct Directory), NULL, NULL);
				POBJ_ALLOC(pop, &D_RO(_dir)->segment, TOID(struct Segment), sizeof(TOID(struct Segment)) * D_RO(dir)->capacity * 2, NULL, NULL);

				D_RW(_dir)->initDirectory(D_RO(dir)->depth + 1);
				auto x = (f_hash >> (8 * sizeof(f_hash) - D_RO(dir)->depth));
				for(int i = 0; i < D_RO(dir)->capacity; ++i){
					if(i == x){
						D_RW(D_RW(_dir)->segment)[2 * i] = new_segment;
						D_RW(D_RW(_dir)->segment)[2 * i + 1] = split_segment;
					}
					else{
						D_RW(D_RW(_dir)->segment)[2 * i] = D_RO(d)[i];
						D_RW(D_RW(_dir)->segment)[2 * i + 1] = D_RO(d)[i];
					}
				}
				pmemobj_flush(pop, (char *)&D_RO(D_RO(_dir)->segment)[0], sizeof(TOID(struct Segment)) * D_RO(_dir)->capacity);
				pmemobj_persist(pop, (char*)&_dir, sizeof(struct Directory));
				dir = _dir;
				pmemobj_persist(pop, (char*)&dir, sizeof(TOID(struct Directory)));
			} else { // normal split
				while (!D_RW(dir)->lock()){
					asm("nop");
				}
				// other thread may change the dir by doubling, need to re-calculate x
				auto x = (f_hash >> (8 * sizeof(f_hash) - D_RO(dir)->depth));
				if (D_RO(dir)->depth == D_RO(target)->local_depth + 1) {
					if (x % 2 == 0) {
						D_RW(D_RW(dir)->segment)[x + 1] = split_segment;
						pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x + 1], sizeof(TOID(struct Segment)));
						mfence();
						D_RW(D_RW(dir)->segment)[x] = new_segment;
						pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x], sizeof(TOID(struct Segment)) * 2);
					} else {
						D_RW(D_RW(dir)->segment)[x] = split_segment;
						pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x], sizeof(TOID(struct Segment)));
						mfence();
						D_RW(D_RW(dir)->segment)[x - 1] = new_segment;
						pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x - 1], sizeof(TOID(struct Segment)) * 2);
					}
				} else {
					// there are multiple dir entries pointing to split segment
					int stride = pow(2, D_RO(dir)->depth - target_local_depth);

					auto loc = x - (x % stride);
					for(int i = 0; i < stride / 2; ++i) {
						D_RW(D_RW(dir)->segment)[loc + stride / 2 + i] = split_segment;
					}
					pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[loc + stride / 2], sizeof(TOID(struct Segment)) * stride / 2);
					
					for(int i = 0; i < stride / 2; ++i) {
						D_RW(D_RW(dir)->segment)[loc + i] = new_segment;
					}
					pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[loc], sizeof(TOID(struct Segment)) * stride);
				}
			D_RW(dir)->unlock();
			}
		
	} else {
		// all records in bufnode has been merge to new_segment_dram

		TOID(struct Segment) new_segment;
		POBJ_ALLOC(pop, &new_segment, struct Segment, sizeof(struct Segment), NULL,NULL);
		// persist the new segment
		pmemobj_memcpy(pop, D_RW(new_segment), &old_segment_dram, sizeof(Segment), PMEMOBJ_F_MEM_NONTEMPORAL);
		// wait for directory lock. Replace old segment with new segment
		while (!D_RW(dir)->lock()) {
			asm("nop");
		}

    	auto x = (f_hash >> (8 * sizeof(f_hash) - D_RO(dir)->depth));

		if (D_RO(dir)->depth == D_RO(target)->local_depth) {
			// only need to update one dir entry
			// INFO("Update one directory %lu", x);
			D_RW(D_RW(dir)->segment)[x] = new_segment;
			pmemobj_persist(pop, (char *)&D_RW(D_RW(dir)->segment)[x], sizeof(TOID(struct Segment)));
		} else if (D_RO(dir)->depth == D_RO(target)->local_depth + 1) {
			if (x % 2 == 0) {
				// INFO("Merge. Update two directory %lu, %lu. Segment %lu depth: %u,
				// dir depth: %lu", x, x+1, x, D_RO(target)->local_depth,
				// D_RO(dir)->depth);
				D_RW(D_RW(dir)->segment)[x + 1] = new_segment;
				pmemobj_persist(pop, (char *)&D_RO(D_RO(dir)->segment)[x + 1],
								sizeof(TOID(struct Segment)));
				mfence();
				D_RW(D_RW(dir)->segment)[x] = new_segment;
				pmemobj_persist(pop, (char *)&D_RO(D_RO(dir)->segment)[x],
								sizeof(TOID(struct Segment)) * 2);
			} else {
				// INFO("Merge. Update two directory %lu, %lu. Segment %lu depth: %u,
				// dir depth: %lu", x, x-1, x, D_RO(target)->local_depth,
				// D_RO(dir)->depth);
				D_RW(D_RW(dir)->segment)[x] = new_segment;
				pmemobj_persist(pop, (char *)&D_RO(D_RO(dir)->segment)[x],
								sizeof(TOID(struct Segment)));
				mfence();
				D_RW(D_RW(dir)->segment)[x - 1] = new_segment;
				pmemobj_persist(pop, (char *)&D_RO(D_RO(dir)->segment)[x - 1],
								sizeof(TOID(struct Segment)) * 2);
			}
		} else {
			// there are multiple dir entries pointing to this segment
			// INFO("Update multiple directory");
			int stride = pow(2, D_RO(dir)->depth - D_RO(target)->local_depth);
			auto loc = x - (x % stride);
			for (int i = 0; i < stride / 2; ++i) {
				auto new_x = loc + stride / 2 + i;
				// INFO("Update multiple directory %lu", new_x);
				D_RW(D_RW(dir)->segment)[new_x] = new_segment;
			}
			pmemobj_persist(pop, (char *)&D_RO(D_RO(dir)->segment)[loc + stride / 2],
							sizeof(TOID(struct Segment)) * stride / 2);
		}
    	D_RW(dir)->unlock();
	}

	bufnode->Reset();
}

bool CCEH::Delete(Key_t& key){
    return false;
}

void CCEH::Recovery(PMEMobjpool* pop){
    size_t i = 0;
    while(i < D_RO(dir)->capacity){
	size_t depth_cur = D_RO(D_RO(D_RO(dir)->segment)[i])->local_depth;
	size_t stride = pow(2, D_RO(dir)->depth - depth_cur);
	size_t buddy = i + stride;
	if(buddy == D_RO(dir)->capacity) break;
	for(int j=buddy-1; i<j; j--){
	    if(D_RO(D_RO(D_RO(dir)->segment)[j])->local_depth != depth_cur){
		D_RW(D_RW(dir)->segment)[j] = D_RO(D_RO(dir)->segment)[i];
		pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[i], sizeof(TOID(struct Segment)));
	    }
	}
	i += stride;
    }
    pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[0], sizeof(TOID(struct Segment))*D_RO(dir)->capacity);
}

double CCEH::Utilization(void){
    size_t sum = 0;
    size_t cnt = 0;
    for(int i=0; i<D_RO(dir)->capacity; ++cnt){
	auto target = D_RO(D_RO(dir)->segment)[i];
	int stride = pow(2, D_RO(dir)->depth - D_RO(target)->local_depth);
	auto pattern = (i >> (D_RO(dir)->depth - D_RO(target)->local_depth));
	for(unsigned j=0; j<Segment::kNumSlot; ++j){
	    auto f_hash = h(&D_RO(target)->bucket[j].key, sizeof(Key_t));
	    if(((f_hash >> (8*sizeof(f_hash)-D_RO(target)->local_depth)) == pattern) && (D_RO(target)->bucket[j].key != INVALID)){
		sum++;
	    }
	}
	i += stride;
    }
    return ((double)sum) / ((double)cnt * Segment::kNumSlot)*100.0;
}

size_t CCEH::Capacity(void){
    size_t cnt = 0;
    for(int i=0; i<D_RO(dir)->capacity; cnt++){
	auto target = D_RO(D_RO(dir)->segment)[i];
	int stride = pow(2, D_RO(dir)->depth - D_RO(target)->local_depth);
	i += stride;
    }

    return cnt * Segment::kNumSlot;
}

// for debugging
Value_t CCEH::FindAnyway(Key_t& key){
    for(size_t i=0; i<D_RO(dir)->capacity; ++i){
	for(size_t j=0; j<Segment::kNumSlot; ++j){
	    if(D_RO(D_RO(D_RO(dir)->segment)[i])->bucket[j].key == key){
		cout << "segment(" << i << ")" << endl;
		cout << "global_depth(" << D_RO(dir)->depth << "), local_depth(" << D_RO(D_RO(D_RO(dir)->segment)[i])->local_depth << ")" << endl;
		cout << "pattern: " << bitset<sizeof(int64_t)>(i >> (D_RO(dir)->depth - D_RO(D_RO(D_RO(dir)->segment)[i])->local_depth)) << endl;
		cout << "Key MSB: " << bitset<sizeof(int64_t)>(h(&key, sizeof(key)) >> (8*sizeof(key) - D_RO(D_RO(D_RO(dir)->segment)[i])->local_depth)) << endl;
		return D_RO(D_RO(D_RO(dir)->segment)[i])->bucket[j].value;
	    }
	}
    }
    return NONE;
}

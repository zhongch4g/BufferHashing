#include <gflags/gflags.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <bitset>
#include <cassert>
#include <condition_variable>  // std::condition_variable
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <iterator>
#include <mutex>  // std::mutex
#include <sstream>
#include <thread>  // std::thread
#include <vector>

#include "CCEH_buflog.h"
#include "cceh_util.h"
#include "histogram.h"

#define likely(x) (__builtin_expect (false || (x), true))
#define unlikely(x) (__builtin_expect (x, 0))

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;
using namespace cceh_buflog;

DEFINE_int32 (initsize, 16, "initial capacity in million");
DEFINE_string (filepath, "/mnt/pmem/objpool.data", "");
DEFINE_uint32 (ins_num, 1, "Number of CCEH instance");
DEFINE_uint32 (batch, 1000000, "report batch");
DEFINE_uint32 (readtime, 0, "if 0, then we read all keys");
DEFINE_uint32 (thread, 1, "");
DEFINE_double (report_interval, 0, "Report interval in seconds");
DEFINE_uint64 (stats_interval, 1000000, "Report interval in ops");
DEFINE_uint64 (value_size, 8, "The value size");
DEFINE_uint64 (num, 10 * 1000000LU, "Number of total record");
DEFINE_uint64 (read, 0, "Number of read operations");
DEFINE_uint64 (write, 1 * 1000000, "Number of read operations");
DEFINE_bool (hist, false, "");
DEFINE_string (benchmarks, "load,readall", "");
DEFINE_int32 (gChoice, 0, "Write Percentage");
DEFINE_bool (withlog, false, "");
DEFINE_bool (recovery, false, "");

size_t kLogSize = 10 * 1024 * 1024 * 1024LU;  // 10G
struct LogRoot {
    size_t num;
    PMEMoid log_addrs[64];
};

LogRoot* log_root;
PMEMobjpool* log_pop;

void createLog () {
    // create log for each threads

    std::string filepath = "/mnt/pmem/cceh_buflog_wal";
    remove (filepath.c_str ());

    log_pop = pmemobj_create (filepath.c_str (), POBJ_LAYOUT_NAME (wal), kLogSize * 1.5, 0666);
    if (log_pop == nullptr) {
        std::cerr << "create log file fail. " << filepath << std::endl;
        exit (1);
    }

    PMEMoid g_root = pmemobj_root (log_pop, sizeof (LogRoot));
    log_root = reinterpret_cast<LogRoot*> (pmemobj_direct (g_root));

    size_t log_size = kLogSize / FLAGS_thread;
    for (int i = 0; i < FLAGS_thread; i++) {
        int ret = pmemobj_alloc (log_pop, &log_root->log_addrs[i], log_size, 0, NULL, NULL);
        if (ret) {
            printf ("alloc error for log");
            exit (1);
        }
        // void* log_base_addr = pmemobj_direct (log_root->log_addrs[i]);
        // pmemobj_memset_persist (log_pop, log_base_addr, 0, log_size);
        // _mm_sfence ();
    }
}

void openLog () {
    std::string filepath = "/mnt/pmem/cceh_buflog_wal";
    log_pop = pmemobj_open (filepath.c_str (), POBJ_LAYOUT_NAME (wal));
    if (log_pop == nullptr) {
        std::cerr << "create log file fail. " << filepath << std::endl;
        exit (1);
    }

    PMEMoid g_root = pmemobj_root (log_pop, sizeof (LogRoot));
    log_root = reinterpret_cast<LogRoot*> (pmemobj_direct (g_root));
}

namespace {

class Stats {
public:
    int tid_;
    double start_;
    double finish_;
    double real_finish_;
    double seconds_;
    double ignore_seconds_;
    double next_report_time_;
    double last_op_finish_;
    unsigned last_level_compaction_num_;
    util::HistogramImpl hist_;

    uint64_t done_;
    uint64_t last_report_done_;
    uint64_t last_report_finish_;
    uint64_t next_report_;
    std::string message_;

    Stats () { Start (); }
    explicit Stats (int id) : tid_ (id) { Start (); }

    void Start () {
        start_ = NowMicros ();
        next_report_time_ = start_ + FLAGS_report_interval * 1000000;
        next_report_ = 100;
        last_op_finish_ = start_;
        last_report_done_ = 0;
        last_report_finish_ = start_;
        last_level_compaction_num_ = 0;
        done_ = 0;
        seconds_ = 0;
        finish_ = start_;
        real_finish_ = 0;
        message_.clear ();
        hist_.Clear ();
    }

    void Merge (const Stats& other) {
        hist_.Merge (other.hist_);
        done_ += other.done_;
        seconds_ += other.seconds_;
        if (other.start_ < start_) start_ = other.start_;
        if (other.finish_ > finish_) finish_ = other.finish_;

        // Just keep the messages from one thread
        if (message_.empty ()) message_ = other.message_;
    }

    void Stop () {
        finish_ = !real_finish_ ? NowMicros () : real_finish_;
        seconds_ = (finish_ - start_) * 1e-6;
    }

    void StartSingleOp () { last_op_finish_ = NowMicros (); }

    void PrintSpeed () {
        uint64_t now = NowMicros ();
        int64_t usecs_since_last = now - last_report_finish_;

        std::string cur_time = TimeToString (now / 1000000);
        // printf (
        //     "%s ... thread %d: (%lu,%lu) ops and "
        //     "( %.1f,%.1f ) ops/second in (%.4f,%.4f) seconds\n",
        //     cur_time.c_str (), tid_, last_report_done_, done_,
        //     (done_ - last_report_done_) / (usecs_since_last / 1000000.0),
        //     done_ / ((now - start_) / 1000000.0), (now - last_report_finish_) / 1000000.0,
        //     (now - start_) / 1000000.0);

        // each epoch speed
        printf ("[TIME]%d,%lu,%lu,%.4f,%.4f\n", tid_, last_report_done_, done_,
                usecs_since_last / 1000000.0, (now - start_) / 1000000.0);
        last_report_finish_ = now;
        last_report_done_ = done_;
        fflush (stdout);
    }

    static void AppendWithSpace (std::string* str, const std::string& msg) {
        if (msg.empty ()) return;
        if (!str->empty ()) {
            str->push_back (' ');
        }
        str->append (msg.data (), msg.size ());
    }

    void AddMessage (const std::string& msg) { AppendWithSpace (&message_, msg); }

    inline bool FinishedBatchOp (size_t batch) {
        double now = NowNanos ();
        last_op_finish_ = now;
        done_ += batch;
        if (unlikely (done_ >= next_report_)) {
            if (next_report_ < 1000)
                next_report_ += 100;
            else if (next_report_ < 5000)
                next_report_ += 500;
            else if (next_report_ < 10000)
                next_report_ += 1000;
            else if (next_report_ < 50000)
                next_report_ += 5000;
            else if (next_report_ < 100000)
                next_report_ += 10000;
            else if (next_report_ < 500000)
                next_report_ += 50000;
            else
                next_report_ += 100000;
            fprintf (stderr, "... finished %llu ops%30s\r", (unsigned long long)done_, "");

            if (FLAGS_report_interval == 0 && (done_ % FLAGS_stats_interval) == 0) {
                // PrintSpeed ();
                return true;
            }
            fflush (stderr);
            fflush (stdout);
        }

        if (FLAGS_report_interval && NowMicros () > next_report_time_) {
            next_report_time_ += FLAGS_report_interval * 1000000;
            // PrintSpeed ();
            return false;
        }
        return true;
    }

    inline void FinishedSingleOp () {
        double now = NowNanos ();
        last_op_finish_ = now;

        done_++;
        if (done_ >= next_report_) {
            if (next_report_ < 1000)
                next_report_ += 100;
            else if (next_report_ < 5000)
                next_report_ += 500;
            else if (next_report_ < 10000)
                next_report_ += 1000;
            else if (next_report_ < 50000)
                next_report_ += 5000;
            else if (next_report_ < 100000)
                next_report_ += 10000;
            else if (next_report_ < 500000)
                next_report_ += 50000;
            else
                next_report_ += 100000;
            fprintf (stderr, "... finished %llu ops%30s\r", (unsigned long long)done_, "");

            if (FLAGS_report_interval == 0 && (done_ % FLAGS_stats_interval) == 0) {
                // PrintSpeed ();
                return;
            }
            fflush (stderr);
            fflush (stdout);
        }

        if (FLAGS_report_interval != 0 && NowNanos () > next_report_time_) {
            next_report_time_ += FLAGS_report_interval * 1000000;
            // PrintSpeed ();
        }
    }

    std::string TimeToString (uint64_t secondsSince1970) {
        const time_t seconds = (time_t)secondsSince1970;
        struct tm t;
        int maxsize = 64;
        std::string dummy;
        dummy.reserve (maxsize);
        dummy.resize (maxsize);
        char* p = &dummy[0];
        localtime_r (&seconds, &t);
        snprintf (p, maxsize, "%04d/%02d/%02d-%02d:%02d:%02d ", t.tm_year + 1900, t.tm_mon + 1,
                  t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec);
        return dummy;
    }

    void Report (const Slice& name, bool print_hist = false) {
        // Pretend at least one op was done in case we are running a benchmark
        // that does not call FinishedSingleOp().
        if (done_ < 1) done_ = 1;

        std::string extra;

        AppendWithSpace (&extra, message_);

        double elapsed = (finish_ - start_) * 1e-6;

        double throughput = (double)done_ / elapsed;

        // The throughput data for none buffer cceh
        // printf ("%-12s : %11.3f micros/op %lf Mops/s;%s%s\n", name.ToString ().c_str (),
        //         elapsed * 1e6 / done_, throughput / 1024 / 1024, (extra.empty () ? "" : " "),
        //         extra.c_str ());

        printf ("*operations* %llu\n*real_elapsed* %2.2f\n*throughput* %2.2f\n", done_, elapsed,
                throughput / 1024 / 1024);

        if (print_hist) {
            fprintf (stdout, "Nanoseconds per op:\n%s\n", hist_.ToString ().c_str ());
        }

        fflush (stdout);
        fflush (stderr);
    }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
    std::mutex mu;
    std::condition_variable cv;
    int total;

    // Each thread goes through the following states:
    //    (1) initializing
    //    (2) waiting for others to be initialized
    //    (3) running
    //    (4) done

    int num_initialized;
    int num_done;
    bool start;
    std::atomic<uint64_t> middle_step_done;
    SharedState (int total)
        : total (total), num_initialized (0), num_done (0), start (false), middle_step_done (0) {}
};

class PrivateStates {
public:
    PrivateStates () : lastMinorCompactions (0), minorCompactions (0) {}
    uint32_t getMinorCompactions () { return minorCompactions; }
    uint32_t getLastIncrease () { return minorCompactions - lastMinorCompactions; }
    void setLastMinorCompactions () { lastMinorCompactions = minorCompactions; }
    void fetchMinorCompactions (uint32_t num) { minorCompactions += num; }

private:
    uint32_t lastMinorCompactions;
    uint32_t minorCompactions;
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
    int tid;  // 0..n-1 when running in n threads
    // Random rand;         // Has different seeds for different threads
    Stats stats;
    SharedState* shared;
    YCSBGenerator ycsb_gen;
    PrivateStates privateStates;
    ThreadState (int index) : tid (index), stats (index) {}
};

class Duration {
public:
    Duration (uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0) {
        max_seconds_ = max_seconds;
        max_ops_ = max_ops;
        ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
        ops_ = 0;
        start_at_ = NowMicros ();
    }

    inline int64_t GetStage () { return std::min (ops_, max_ops_ - 1) / ops_per_stage_; }

    inline bool Done (int64_t increment) {
        if (increment <= 0) increment = 1;  // avoid Done(0) and infinite loops
        ops_ += increment;

        if (max_seconds_) {
            // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
            auto granularity = 1000;
            if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
                uint64_t now = NowMicros ();
                return ((now - start_at_) / 1000000) >= max_seconds_;
            } else {
                return false;
            }
        } else {
            return ops_ > max_ops_;
        }
    }

    inline int64_t Ops () { return ops_; }

private:
    uint64_t max_seconds_;
    int64_t max_ops_;
    int64_t ops_per_stage_;
    int64_t ops_;
    uint64_t start_at_;
};

#if defined(__linux)
static std::string TrimSpace (std::string s) {
    size_t start = 0;
    while (start < s.size () && isspace (s[start])) {
        start++;
    }
    size_t limit = s.size ();
    while (limit > start && isspace (s[limit - 1])) {
        limit--;
    }
    return std::string (s.data () + start, limit - start);
}
#endif

}  // namespace
#define CREATE_MODE_RW (S_IWRITE | S_IREAD)
#define POOL_SIZE (1073741824L * 100L)  // 20GB
class Benchmark {
public:
    uint64_t num_;
    int value_size_;
    size_t reads_;
    size_t writes_;
    RandomKeyTrace* key_trace_;
    size_t trace_size_;
    PMEMobjpool* pop_;
    TOID (CCEH) hashtable_;
    uint32_t ins_num_;
    std::atomic<uint64_t> thread_counter;
    uint64_t batch_counter;
    Benchmark ()
        : num_ (FLAGS_num),
          value_size_ (FLAGS_value_size),
          reads_ (FLAGS_read),
          writes_ (FLAGS_write),
          key_trace_ (nullptr),
          hashtable_ (OID_NULL),
          thread_counter (0),
          batch_counter (0) {
        const size_t initialSize = 1024 * FLAGS_initsize;  // 16 million initial
        if (!FLAGS_recovery) {
            remove (FLAGS_filepath.c_str ());  // delete the mapped file.
            pop_ = pmemobj_create (FLAGS_filepath.c_str (), "CCEH", POOL_SIZE, 0666);
            if (!pop_) {
                perror ("pmemoj_create");
                exit (1);
            }
            hashtable_ = POBJ_ROOT (pop_, CCEH);
            if (!FLAGS_withlog) {
                printf ("CCEH-BUFLOG \n");
                D_RW (hashtable_)->initCCEH (pop_, initialSize);
            } else {
                printf ("CCEH-BUFLOG-LOG \n");
                createLog ();
                D_RW (hashtable_)->initCCEH (pop_, initialSize, FLAGS_thread, log_root->log_addrs);
            }
        } else {
            printf ("CCEH-BUFLOG-RECOVERY \n");
            pop_ = pmemobj_open (FLAGS_filepath.c_str (), "CCEH");
            hashtable_ = POBJ_ROOT (pop_, CCEH);
            openLog ();
            D_RW (hashtable_)->Recovery (pop_, FLAGS_thread);
        }
    }

    ~Benchmark () {}

    void Run () {
        trace_size_ = FLAGS_num;
        key_trace_ = new RandomKeyTrace (trace_size_);  // a 1 dim trace_size_ long vector
        if (reads_ == 0) {
            reads_ = key_trace_->count_;
            FLAGS_read = key_trace_->count_;
        }
        // current program info
        PrintHeader ();
        bool fresh_db = true;
        // run benchmark
        bool print_hist = false;
        const char* benchmarks = FLAGS_benchmarks.c_str ();
        while (benchmarks != nullptr) {
            int thread = FLAGS_thread;
            void (Benchmark::*method) (ThreadState*) = nullptr;
            const char* sep = strchr (benchmarks, ',');
            std::string name;
            if (sep == nullptr) {
                name = benchmarks;
                benchmarks = nullptr;
            } else {
                name = std::string (benchmarks, sep - benchmarks);
                benchmarks = sep + 1;
            }
            if (name == "load") {
                fresh_db = true;
                method = &Benchmark::DoWrite;
            } else if (name == "recovery") {
                fresh_db = true;
                method = &Benchmark::DoRecovery;
            } else if (name == "loadlat") {
                fresh_db = true;
                print_hist = true;
                method = &Benchmark::DoWriteLat;
            } else if (name == "allloadfactor") {
                fresh_db = true;
                method = &Benchmark::DoLoadFactor;
            } else if (name == "overwrite") {
                fresh_db = false;
                key_trace_->Randomize ();
                method = &Benchmark::DoOverWrite;
            } else if (name == "readrandom") {
                fresh_db = false;
                key_trace_->Randomize ();
                method = &Benchmark::DoRead;
            } else if (name == "readall") {
                fresh_db = false;
                key_trace_->Randomize ();
                method = &Benchmark::DoReadAll;
            } else if (name == "readnon") {
                fresh_db = false;
                key_trace_->Randomize ();
                method = &Benchmark::DoReadNon;
            } else if (name == "readlat") {
                fresh_db = false;
                print_hist = true;
                key_trace_->Randomize ();
                method = &Benchmark::DoReadLat;
            } else if (name == "readnonlat") {
                fresh_db = false;
                print_hist = true;
                key_trace_->Randomize ();
                method = &Benchmark::DoReadNonLat;
            } else if (name == "readwhilewriting") {
                fresh_db = false;
                key_trace_->Randomize ();
                method = &Benchmark::ReadWhileWriting;
            } else if (name == "ycsba") {
                fresh_db = false;
                key_trace_->Randomize ();
                method = &Benchmark::YCSBA;
            } else if (name == "ycsbb") {
                fresh_db = false;
                key_trace_->Randomize ();
                method = &Benchmark::YCSBB;
            } else if (name == "ycsbc") {
                fresh_db = false;
                key_trace_->Randomize ();
                method = &Benchmark::YCSBC;
            } else if (name == "ycsbd") {
                fresh_db = false;
                method = &Benchmark::YCSBD;
            } else if (name == "ycsbf") {
                fresh_db = false;
                key_trace_->Randomize ();
                method = &Benchmark::YCSBF;
            } else if (name == "ycsbg") {
                fresh_db = false;
                key_trace_->Randomize ();
                method = &Benchmark::YCSBG;
            }

            IPMWatcher watcher (name);
            if (method != nullptr) RunBenchmark (thread, name, method, print_hist);
        }
    }

    void DoRead (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("DoRead lack key_trace_ initialization.");
            return;
        }

        size_t start_offset = random () % trace_size_;
        auto key_iterator = key_trace_->trace_at (start_offset, trace_size_);
        size_t not_find = 0;

        Duration duration (FLAGS_readtime, reads_);
        thread->stats.Start ();
        while (!duration.Done (batch) && key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t ikey = key_iterator.Next ();
                auto ret = D_RW (hashtable_)->Get (ikey);
                if (ret != reinterpret_cast<Value_t> (ikey)) {
                    not_find++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(num: %lu, not find: %lu)", reads_, not_find);
        thread->stats.AddMessage (buf);
    }

    void DoReadAll (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("DoReadAll lack key_trace_ initialization.");
            return;
        }
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);
        // printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
        //         start_offset + interval);

        size_t not_find = 0;

        Duration duration (FLAGS_readtime, reads_);
        thread->stats.Start ();
        while (!duration.Done (batch) && key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t ikey = key_iterator.Next ();
                auto ret = D_RW (hashtable_)->Get (ikey);
                if (ret != reinterpret_cast<Value_t> (ikey)) {
                    not_find++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(num: %lu, not find: %lu)", interval, not_find);
        if (not_find)
            printf ("thread %2d num: %lu, not find: %lu\n", thread->tid, interval, not_find);
        thread->stats.AddMessage (buf);
    }

    void DoReadNon (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("DoRead lack key_trace_ initialization.");
            return;
        }
        // size_t start_offset = random () % trace_size_;
        // auto key_iterator = key_trace_->trace_at (start_offset, trace_size_);
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);
        size_t not_find = 0;

        Duration duration (FLAGS_readtime, reads_);
        thread->stats.Start ();
        while (!duration.Done (batch) && key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t ikey = key_iterator.Next () + num_;
                auto ret = D_RW (hashtable_)->Get (ikey);
                if (ret != reinterpret_cast<Value_t> (ikey)) {
                    not_find++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(num: %lu, not find: %lu)", reads_, not_find);
        thread->stats.AddMessage (buf);
    }

    void DoReadLat (ThreadState* thread) {
        if (key_trace_ == nullptr) {
            perror ("DoReadLat lack key_trace_ initialization.");
            return;
        }
        size_t start_offset = random () % trace_size_;
        auto key_iterator = key_trace_->trace_at (start_offset, trace_size_);
        size_t not_find = 0;

        Duration duration (FLAGS_readtime, reads_);
        thread->stats.Start ();
        while (!duration.Done (1) && key_iterator.Valid ()) {
            size_t ikey = key_iterator.Next ();

            auto time_start = NowNanos ();
            auto ret = D_RW (hashtable_)->Get (ikey);
            auto time_duration = NowNanos () - time_start;

            thread->stats.hist_.Add (time_duration);
            if (ret != reinterpret_cast<Value_t> (ikey)) {
                not_find++;
            }
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(num: %lu, not find: %lu)", reads_, not_find);
        thread->stats.AddMessage (buf);
    }

    void ReadWhileWriting (ThreadState* thread) {
        if (key_trace_ == nullptr) {
            perror ("ReadWhileWriting lack key_trace_ initialization.");
            return;
        }

        // Only one of the thread is writing
        if (thread->tid > 0) {
            DoRead (thread);
        } else {
            uint64_t batch = FLAGS_batch;
            // Special thread that keeps writing until other threads are done.
            size_t interval = num_;
            auto key_iterator = key_trace_->iterate_between (0, 0 + interval);

            thread->stats.Start ();
            while (key_iterator.Valid ()) {
                uint64_t j = 0;
                for (; j < batch && key_iterator.Valid (); j++) {
                    size_t ikey = key_iterator.Next ();
                    D_RW (hashtable_)->Insert (pop_, ikey, reinterpret_cast<Value_t> (ikey));
                }
                // Do not count any of the preceding work/delay in stats.
                thread->stats.FinishedBatchOp (j);
            }
        }
    }

    void DoReadNonLat (ThreadState* thread) {
        if (key_trace_ == nullptr) {
            perror ("DoReadLat lack key_trace_ initialization.");
            return;
        }
        size_t start_offset = random () % trace_size_;
        auto key_iterator = key_trace_->trace_at (start_offset, trace_size_);
        size_t not_find = 0;

        Duration duration (FLAGS_readtime, reads_);
        thread->stats.Start ();
        while (!duration.Done (1) && key_iterator.Valid ()) {
            size_t ikey = key_iterator.Next () + num_;

            auto time_start = NowNanos ();
            auto ret = D_RW (hashtable_)->Get (ikey);
            auto time_duration = NowNanos () - time_start;

            thread->stats.hist_.Add (time_duration);
            if (ret != reinterpret_cast<Value_t> (ikey)) {
                not_find++;
            }
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(num: %lu, not find: %lu)", reads_, not_find);
        thread->stats.AddMessage (buf);
    }

    void DoWrite (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("DoWrite lack key_trace_ initialization.");
            return;
        }
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);
        // printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
        // start_offset + interval);
        thread->stats.Start ();
        std::string val (value_size_, 'v');
        size_t inserted = 0;
        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                inserted++;
                size_t ikey = key_iterator.Next ();
                if (FLAGS_withlog) {
                    D_RW (hashtable_)
                        ->InsertWithLog (pop_, ikey, reinterpret_cast<Value_t> (ikey), thread->tid);
                } else {
                    D_RW (hashtable_)->Insert (pop_, ikey, reinterpret_cast<Value_t> (ikey));
                }
            }
            bool res = thread->stats.FinishedBatchOp (j);
            if (!res) {
                // time epoch
                // if (thread->tid == 0) {
                //     printf ("[#segments]%lu\n", D_RW (hashtable_)->curSegmentNum.load ());
                // }
            }
            // std::unique_lock<std::mutex> lck (thread->shared->mu);
            // thread->shared->middle_step_done.fetch_add (1, std::memory_order_relaxed);
            // if (thread->shared->middle_step_done.load () >= thread->shared->total) {
            //     printf ("[#segments]%lu\n", D_RW (hashtable_)->curSegmentNum.load ());
            //     // D_RW (hashtable_)
            //     //     ->ScanShadow (FLAGS_thread * batch *
            //     //                   batch_counter);  // Check each segments writes
            //     thread_counter.fetch_add (1, std::memory_order_relaxed);
            //     thread->shared->cv.notify_all ();
            //     thread_counter.store (0);
            //     thread->shared->middle_step_done.store (0);
            // }
            // while (thread_counter.load () >= thread->shared->total) {
            //     thread->shared->cv.wait (lck);
            // }
        }

        thread->stats.real_finish_ = NowMicros ();

        if (thread->tid >= 0) {
            thread_counter.fetch_add (1);
        }
        printf ("%lu, %lu\n", thread_counter.load (), FLAGS_thread);
        if (thread_counter.load () >= FLAGS_thread) {
            D_RW (hashtable_)->flushAllBuffers (pop_);
        }
        return;
    }

    void DoRecovery (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("DoWrite lack key_trace_ initialization.");
            return;
        }

        thread->stats.Start ();

        D_RW (hashtable_)->BufferRecovery (pop_, FLAGS_thread, thread->tid, log_root->log_addrs);

        thread->stats.real_finish_ = NowMicros ();

        return;
    }

    void DoWriteLat (ThreadState* thread) {
        if (key_trace_ == nullptr) {
            perror ("DoWriteLat lack key_trace_ initialization.");
            return;
        }
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);
        // printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
        //         start_offset + interval);
        thread->stats.Start ();
        while (key_iterator.Valid ()) {
            size_t ikey = key_iterator.Next ();

            auto time_start = NowNanos ();
            D_RW (hashtable_)->Insert (pop_, ikey, reinterpret_cast<Value_t> (ikey));
            auto time_duration = NowNanos () - time_start;
            thread->stats.hist_.Add (time_duration);
        }
        return;
    }

    void DoLoadFactor (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("DoLoadFactor lack key_trace_ initialization.");
            return;
        }
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);
        // printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
        //         start_offset + interval);
        size_t inserted = 0;
        thread->stats.Start ();
        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t ikey = key_iterator.Next ();
                D_RW (hashtable_)->Insert (pop_, ikey, reinterpret_cast<Value_t> (ikey));
                inserted++;
            }
            thread->stats.FinishedBatchOp (j);
            printf ("Load factor: %.3f\n", (double)(inserted) / D_RW (hashtable_)->Capacity ());
        }
        return;
    }

    void DoOverWrite (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("DoOverWrite lack key_trace_ initialization.");
            return;
        }
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);
        // printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
        //         start_offset + interval);
        thread->stats.Start ();
        std::string val (value_size_, 'v');
        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t ikey = key_iterator.Next ();
                D_RW (hashtable_)->Insert (pop_, ikey, reinterpret_cast<Value_t> (ikey));
            }
            thread->stats.FinishedBatchOp (j);
        }
        return;
    }

    void YCSBA (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("YCSBA lack key_trace_ initialization.");
            return;
        }
        size_t find = 0;
        size_t insert = 0;
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);
        // printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
        //         start_offset + interval);
        thread->stats.Start ();

        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next ();
                if (thread->ycsb_gen.NextA () == kYCSB_Write) {
                    D_RW (hashtable_)->Insert (pop_, key, reinterpret_cast<Value_t> (key));
                    insert++;
                } else {
                    auto ret = D_RW (hashtable_)->Get (key);
                    find++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(insert: %lu, read: %lu)", insert, find);
        thread->stats.AddMessage (buf);
        return;
    }

    void YCSBB (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("YCSBB lack key_trace_ initialization.");
            return;
        }
        size_t find = 0;
        size_t insert = 0;
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);
        // printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
        //         start_offset + interval);
        thread->stats.Start ();

        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next ();
                if (thread->ycsb_gen.NextG (FLAGS_gChoice) == kYCSB_Write) {
                    D_RW (hashtable_)->Insert (pop_, key, reinterpret_cast<Value_t> (key));
                    insert++;
                } else {
                    D_RW (hashtable_)->Get (key);
                    find++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(insert: %lu, read: %lu)", insert, find);
        thread->stats.AddMessage (buf);
        return;
    }

    void YCSBC (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("YCSBC lack key_trace_ initialization.");
            return;
        }
        size_t find = 0;
        size_t insert = 0;
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);
        // printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
        //         start_offset + interval);
        thread->stats.Start ();

        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next ();
                auto ret = D_RW (hashtable_)->Get (key);
                if (ret == reinterpret_cast<Value_t> (key)) {
                    find++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(insert: %lu, read: %lu)", insert, find);
        thread->stats.AddMessage (buf);
        return;
    }

    void YCSBD (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("YCSBD lack key_trace_ initialization.");
            return;
        }
        size_t find = 0;
        size_t insert = 0;
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        // Read the latest 20%
        auto key_iterator =
            key_trace_->iterate_between (start_offset + 0.8 * interval, start_offset + interval);
        // printf ("thread %2d, between %lu - %lu\n", thread->tid,
        //         (size_t)(start_offset + 0.8 * interval), start_offset + interval);
        thread->stats.Start ();

        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next ();
                auto ret = D_RW (hashtable_)->Get (key);
                if (ret == reinterpret_cast<Value_t> (key)) {
                    find++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(insert: %lu, read: %lu)", insert, find);
        thread->stats.AddMessage (buf);
        return;
    }

    void YCSBF (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("YCSBF lack key_trace_ initialization.");
            return;
        }
        size_t find = 0;
        size_t insert = 0;
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);
        // printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
        //         start_offset + interval);
        thread->stats.Start ();

        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next ();
                if (thread->ycsb_gen.NextF () == kYCSB_Read) {
                    auto ret = D_RW (hashtable_)->Get (key);
                    if (ret == reinterpret_cast<Value_t> (key)) {
                        find++;
                    }
                } else {
                    D_RW (hashtable_)->Get (key);
                    D_RW (hashtable_)->Insert (pop_, key, reinterpret_cast<Value_t> (key));
                    insert++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        char buf[100];
        snprintf (buf, sizeof (buf), "(read_modify: %lu, read: %lu)", insert, find);
        thread->stats.AddMessage (buf);
        return;
    }

    void YCSBG (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("YCSBG lack key_trace_ initialization.");
            return;
        }
        size_t find = 0;
        size_t insert = 0;
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);
        // printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
        //         start_offset + interval);
        thread->stats.Start ();

        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next ();
                if (thread->ycsb_gen.NextG (FLAGS_gChoice) == kYCSB_Write) {
                    D_RW (hashtable_)->Insert (pop_, key, reinterpret_cast<Value_t> (key));
                    insert++;
                } else {
                    auto ret = D_RW (hashtable_)->Get (key);
                    find++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        // char buf[100];
        // snprintf (buf, sizeof (buf), "(insert: %lu, read: %lu)", insert, find);
        // thread->stats.AddMessage (buf);
        return;
    }

private:
    struct ThreadArg {
        Benchmark* bm;
        SharedState* shared;
        ThreadState* thread;
        void (Benchmark::*method) (ThreadState*);
    };

    static void ThreadBody (void* v) {
        ThreadArg* arg = reinterpret_cast<ThreadArg*> (v);
        SharedState* shared = arg->shared;
        ThreadState* thread = arg->thread;
        {
            std::unique_lock<std::mutex> lck (shared->mu);
            shared->num_initialized++;
            if (shared->num_initialized >= shared->total) {
                shared->cv.notify_all ();
            }
            while (!shared->start) {
                shared->cv.wait (lck);
            }
        }

        thread->stats.Start ();
        (arg->bm->*(arg->method)) (thread);
        thread->stats.Stop ();

        {
            std::unique_lock<std::mutex> lck (shared->mu);
            shared->num_done++;
            if (shared->num_done >= shared->total) {
                shared->cv.notify_all ();
            }
        }
    }

    void RunBenchmark (int thread_num, const std::string& name,
                       void (Benchmark::*method) (ThreadState*), bool print_hist) {
        SharedState shared (thread_num);
        ThreadArg* arg = new ThreadArg[thread_num];
        std::thread server_threads[thread_num];
        for (int i = 0; i < thread_num; i++) {
            arg[i].bm = this;
            arg[i].method = method;
            arg[i].shared = &shared;
            arg[i].thread = new ThreadState (i);
            arg[i].thread->shared = &shared;
            server_threads[i] = std::thread (ThreadBody, &arg[i]);
        }

        std::unique_lock<std::mutex> lck (shared.mu);
        while (shared.num_initialized < thread_num) {
            shared.cv.wait (lck);
        }

        shared.start = true;
        shared.cv.notify_all ();
        while (shared.num_done < thread_num) {
            shared.cv.wait (lck);
        }

        for (int i = 1; i < thread_num; i++) {
            arg[0].thread->stats.Merge (arg[i].thread->stats);
        }
        arg[0].thread->stats.Report (name, print_hist);

        for (auto& th : server_threads) th.join ();
    }

    void PrintEnvironment () {
#if defined(__linux)
        time_t now = time (nullptr);
        fprintf (stderr, "Date:                  %s", ctime (&now));  // ctime() adds newline

        FILE* cpuinfo = fopen ("/proc/cpuinfo", "r");
        if (cpuinfo != nullptr) {
            char line[1000];
            int num_cpus = 0;
            std::string cpu_type;
            std::string cache_size;
            while (fgets (line, sizeof (line), cpuinfo) != nullptr) {
                const char* sep = strchr (line, ':');
                if (sep == nullptr) {
                    continue;
                }
                std::string key = TrimSpace (std::string (line, sep - 1 - line));
                std::string val = TrimSpace (std::string (sep + 1));
                if (key == "model name") {
                    ++num_cpus;
                    cpu_type = val;
                } else if (key == "cache size") {
                    cache_size = val;
                }
            }
            fclose (cpuinfo);
            fprintf (stderr, "CPU:                   %d * %s\n", num_cpus, cpu_type.c_str ());
            fprintf (stderr, "CPUCache:              %s\n", cache_size.c_str ());
        }
#endif
    }

    void PrintHeader () {
        fprintf (stdout, "------------------------------------------------\n");
        PrintEnvironment ();
        fprintf (stdout, "HashType:              %s\n", "CCEH buflog");
        fprintf (stdout, "Init Capacity:         %lu\n", D_RW (hashtable_)->Capacity ());
        fprintf (stdout, "Entries:               %lu\n", (uint64_t)num_);
        fprintf (stdout, "Trace size:            %lu\n", (uint64_t)trace_size_);
        fprintf (stdout, "Read:                  %lu \n", (uint64_t)FLAGS_read);
        fprintf (stdout, "Write:                 %lu \n", (uint64_t)FLAGS_write);
        fprintf (stdout, "Thread:                %lu \n", (uint64_t)FLAGS_thread);
        fprintf (stdout, "Report interval:       %lu s\n", (uint64_t)FLAGS_report_interval);
        fprintf (stdout, "Stats interval:        %lu records\n", (uint64_t)FLAGS_stats_interval);
        fprintf (stdout, "benchmarks:            %s\n", FLAGS_benchmarks.c_str ());
        fprintf (stdout, "------------------------------------------------\n");
    }
};

int main (int argc, char* argv[]) {
    ParseCommandLineFlags (&argc, &argv, true);

    int sds_write_value = 0;
    pmemobj_ctl_set (NULL, "sds.at_create", &sds_write_value);

    if (1 == FLAGS_ins_num) {
        Benchmark benchmark;
        benchmark.Run ();
    }
    // else {
    //     Benchmark benchmark (FLAGS_ins_num);
    //     benchmark.Run ();
    // }
    return 0;
}

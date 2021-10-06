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

#include "CCEH_BUF_Exchange.h"
#include "cceh_util.h"
#include "histogram.h"

#define likely(x) (__builtin_expect (false || (x), true))
#define unlikely(x) (__builtin_expect (x, 0))

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

DEFINE_int32 (initsize, 16, "initial capacity in million");
DEFINE_string (filepath, "/mnt/pmem0/objpool.data", "");
DEFINE_uint32 (ins_num, 1, "Number of CCEH instance");
DEFINE_uint32 (batch, 1000000, "report batch");
DEFINE_uint32 (readtime, 0, "if 0, then we read all keys");
DEFINE_uint32 (thread, 1, "");
DEFINE_uint64 (report_interval, 0, "Report interval in seconds");
DEFINE_uint64 (stats_interval, 10000000, "Report interval in ops");
DEFINE_uint64 (value_size, 8, "The value size");
DEFINE_uint64 (num, 10 * 1000000LU, "Number of total record");
DEFINE_uint64 (read, 0, "Number of read operations");
DEFINE_uint64 (write, 1 * 1000000, "Number of read operations");
DEFINE_bool (hist, false, "");
DEFINE_string (benchmarks, "load,readall", "");
DEFINE_uint32 (writeThreads, 1,
               "For readwhilewriting, determine how many write threads out of 16 threads.");
DEFINE_double (bufferRate, 0, "");
DEFINE_int32 (bufferNum, -1, "");

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
        printf (
            "%s ... thread %d: (%lu,%lu) ops and "
            "( %.1f,%.1f ) ops/second in (%.4f,%.4f) seconds\n",
            cur_time.c_str (), tid_, done_ - last_report_done_, done_,
            (done_ - last_report_done_) / (usecs_since_last / 1000000.0),
            done_ / ((now - start_) / 1000000.0), (now - last_report_finish_) / 1000000.0,
            (now - start_) / 1000000.0);

        // each epoch speed
        // printf ("%d,%lu,%lu,%.4f,%.4f\n", tid_, last_report_done_, done_,
        //         usecs_since_last / 1000000.0,
        //         (done_ - last_report_done_) / (usecs_since_last / 1000000.0) / 1024 / 1024);
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

    inline void FinishedBatchOp (size_t batch) {
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
                PrintSpeed ();
                return;
            }
            fflush (stderr);
            fflush (stdout);
        }

        if (FLAGS_report_interval != 0 && NowNanos () > next_report_time_) {
            next_report_time_ += FLAGS_report_interval * 1000000;
            PrintSpeed ();
        }
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
                PrintSpeed ();
                return;
            }
            fflush (stderr);
            fflush (stdout);
        }

        if (FLAGS_report_interval != 0 && NowNanos () > next_report_time_) {
            next_report_time_ += FLAGS_report_interval * 1000000;
            PrintSpeed ();
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

        printf ("%llu operations, %2.2f real_elapsed \n", done_, elapsed);

        if (print_hist) {
            fprintf (stdout, "Nanoseconds per op:\n%s\n", hist_.ToString ().c_str ());
        }

        // FILE *fp;
        // fp = fopen("result-8k/8k-buffer.txt","a");
        // fprintf(fp,"%lf\n",throughput / 1024 / 1024);
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
    int middle_step_done;
    SharedState (int total)
        : total (total), num_initialized (0), num_done (0), start (false), middle_step_done (0) {}
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
    int tid;  // 0..n-1 when running in n threads
    // Random rand;         // Has different seeds for different threads
    Stats stats;
    SharedState* shared;
    YCSBGenerator ycsb_gen;
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

#define POOL_SIZE (1073741824L * 100L)  // 20GB
class Benchmark {
public:
    uint64_t num_;
    int value_size_;
    size_t reads_;
    size_t writes_;
    RandomKeyTrace* key_trace_;
    RandomKeyTrace* preload_key_trace_;
    size_t trace_size_;
    PMEMobjpool* pop_;
    PMEMobjpool* pop0_;
    PMEMobjpool* pop1_;
    std::vector<PMEMobjpool*> vpop_;
    TOID (CCEH) hashtable_;
    TOID (CCEH) hashtable0_;
    TOID (CCEH) hashtable1_;
    std::vector<TOID (CCEH)> vhashtable_;
    uint32_t ins_num_;
    Benchmark ()
        : num_ (FLAGS_num),
          value_size_ (FLAGS_value_size),
          reads_ (FLAGS_read),
          writes_ (FLAGS_write),
          key_trace_ (nullptr),
          preload_key_trace_ (nullptr),
          hashtable_ (OID_NULL) {
        remove (FLAGS_filepath.c_str ());  // delete the mapped file.
        pop_ = pmemobj_create (FLAGS_filepath.c_str (), "CCEH", POOL_SIZE, 0666);
        if (!pop_) {
            perror ("pmemoj_create");
            exit (1);
        }

        const size_t initialSize = 1024 * FLAGS_initsize;  // 16 million initial
        hashtable_ = POBJ_ROOT (pop_, CCEH);
        // D_RW (hashtable_)->initCCEH (pop_, initialSize);
        printf ("Single CCEH \n");
        // D_RW (hashtable_)->initCCEH (pop_, initialSize, 4, -1, 0);  // full buffer
        // D_RW (hashtable_)->initCCEH (pop_, initialSize, 4, 1, 0);  // no buffer
        // D_RW (hashtable_)->initCCEH (pop_, initialSize, 4, 1, FLAGS_bufferRate);
        D_RW (hashtable_)->initCCEH (pop_, initialSize, 4, FLAGS_bufferNum, FLAGS_bufferRate);
    }

    /*
        Benchmark (uint32_t ins_num)
            : num_ (FLAGS_num),
              value_size_ (FLAGS_value_size),
              reads_ (FLAGS_read),
              writes_ (FLAGS_write),
              key_trace_ (nullptr),
              preload_key_trace_ (nullptr),
              hashtable_ (OID_NULL),
              ins_num_ (FLAGS_ins_num) {
            const size_t initialSize = 1024 * FLAGS_initsize;  // 16 million initial
            std::string file_path1 = "/mnt/pmem";
            std::string file_path2 = "/objpool.data";
            std::string file_path;
            int32_t bufferSizeFactorConfig[] = {4, 4};
            int32_t kBufNumMaxConfig[] = {70000, 70000};  // set it to -1 means full of buffer
            double bufferRateConfig[] = {0, 0};  // kbufnum -1 and rate 0 means no buffer at all

            for (uint32_t n = 0; n < ins_num_; n++) {
                // the file path here must be start at pmem0 directory: pmem0, pmem1, ..., pmemN
                file_path = file_path1 + std::to_string (n) + file_path2;
                remove ((file_path).c_str ());
                pop_ = pmemobj_create (file_path.c_str (), "CCEH", POOL_SIZE, 0666);
                if (!pop_) {
                    perror ((file_path + "\npmemoj_create").c_str ());
                    exit (1);
                }
                vpop_.push_back (pop_);
                hashtable_ = POBJ_ROOT (pop_, CCEH);
                D_RW (hashtable_)
                    ->initCCEH (pop_, initialSize, bufferSizeFactorConfig[n], kBufNumMaxConfig[n],
                                bufferRateConfig[n]);
                vhashtable_.push_back (hashtable_);
            }
        }
    */
    Benchmark (uint32_t ins_num)
        : num_ (FLAGS_num),
          value_size_ (FLAGS_value_size),
          reads_ (FLAGS_read),
          writes_ (FLAGS_write),
          key_trace_ (nullptr),
          preload_key_trace_ (nullptr),
          hashtable_ (OID_NULL),
          hashtable0_ (OID_NULL),
          hashtable1_ (OID_NULL),
          ins_num_ (FLAGS_ins_num) {
        const size_t initialSize = 1024 * FLAGS_initsize;  // 16 million initial
        std::string file_path1 = "/mnt/pmem";
        std::string file_path2 = "/objpool.data";
        std::string file_path;
        int32_t bufferSizeFactorConfig[] = {4, 4};
        int32_t kBufNumMaxConfig[] = {30000, 70000};  // set it to -1 means full of buffer
        double bufferRateConfig[] = {0, 0};  // kbufnum -1 and rate 0 means no buffer at all

        file_path = file_path1 + "0" + file_path2;
        printf ("Remove pmem file : %s \n", (file_path).c_str ());
        remove ((file_path).c_str ());
        pop0_ = pmemobj_create (file_path.c_str (), "CCEH", POOL_SIZE, 0666);

        if (!pop0_) {
            perror ((file_path + "\npmemoj_create 0").c_str ());
            exit (1);
        }
        vpop_.push_back (pop0_);
        hashtable0_ = POBJ_ROOT (pop0_, CCEH);
        D_RW (hashtable0_)
            ->initCCEH (pop0_, initialSize, bufferSizeFactorConfig[0], kBufNumMaxConfig[0],
                        bufferRateConfig[0]);
        vhashtable_.push_back (hashtable0_);

        /************************************************************************************/
        printf ("Separate.. \n");
        file_path = file_path1 + "1" + file_path2;
        printf ("Remove pmem file : %s \n", (file_path).c_str ());
        remove ((file_path).c_str ());
        pop1_ = pmemobj_create (file_path.c_str (), "CCEH", POOL_SIZE, 0666);

        if (!pop1_) {
            perror ((file_path + "\npmemoj_create 1").c_str ());
            exit (1);
        }
        vpop_.push_back (pop1_);
        hashtable1_ = POBJ_ROOT (pop1_, CCEH);
        D_RW (hashtable1_)
            ->initCCEH (pop1_, initialSize, bufferSizeFactorConfig[1], kBufNumMaxConfig[1],
                        bufferRateConfig[1]);
        vhashtable_.push_back (hashtable1_);
    }

    ~Benchmark () {}

    void Run () {
        trace_size_ = FLAGS_num;
        key_trace_ = new RandomKeyTrace (trace_size_);  // a 1 dim trace_size_ long vector
        preload_key_trace_ = new RandomKeyTrace (trace_size_ / 2 / 2);
        if (reads_ == 0) {
            reads_ = key_trace_->count_;
            FLAGS_read = key_trace_->count_;
        }
        // current program info
        // PrintHeader ();
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
            }
            if (name == "loadtest") {
                fresh_db = true;
                method = &Benchmark::DoWriteTest;
            }
            if (name == "loadbuf") {
                fresh_db = true;
                method = &Benchmark::DoWriteBuf;
            }
            if (name == "loadlat") {
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
            } else if (name == "writenwrite") {
                fresh_db = false;
                key_trace_->Randomize ();
                method = &Benchmark::writenwrite;
            } else if (name == "bufferexchange") {
                fresh_db = false;
                key_trace_->Randomize ();
                method = &Benchmark::bufferexchange;
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
        printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
                start_offset + interval);

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
        size_t start_offset = random () % trace_size_;
        auto key_iterator = key_trace_->trace_at (start_offset, trace_size_);
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
        if (thread->tid > FLAGS_writeThreads - 1) {
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
        //         start_offset + interval);
        thread->stats.Start ();
        std::string val (value_size_, 'v');
        size_t inserted = 0;
        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                inserted++;
                size_t ikey = key_iterator.Next ();
                D_RW (hashtable_)->Insert (pop_, ikey, reinterpret_cast<Value_t> (ikey));
            }
            thread->stats.FinishedBatchOp (j);
        }

        thread->stats.real_finish_ = NowMicros ();
        sleep (3);
        if (thread->tid == 0) {
            D_RW (hashtable_)->checkBufferData ();
        }
        return;
    }

    void DoWriteTest (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("DoWrite lack key_trace_ initialization.");
            return;
        }
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);
        // printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
        //         start_offset + interval);
        thread->stats.Start ();
        std::string val (value_size_, 'v');
        size_t inserted = 0;
        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                inserted++;
                size_t ikey = key_iterator.Next ();
                D_RW (hashtable_)->Insert (pop_, ikey, reinterpret_cast<Value_t> (ikey));
            }
            thread->stats.FinishedBatchOp (j);
            if (thread->tid == 0) {
                D_RW (hashtable_)->checkBufferData ();
            }
        }
        // printf ("bufferWrites : %d \n", D_RW (hashtable_)->bufferWrites);
        thread->stats.real_finish_ = NowMicros ();
        sleep (3);
        if (thread->tid == 0) {
            D_RW (hashtable_)->checkBufferData ();
        }
        return;
    }

    void DoWriteBuf (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("DoWrite lack key_trace_ initialization.");
            return;
        }
        size_t interval = num_ / FLAGS_thread;
        size_t start_offset = thread->tid * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);

        thread->stats.Start ();
        std::string val (value_size_, 'v');
        size_t inserted = 0;
        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                inserted++;
                size_t ikey = key_iterator.Next ();
                D_RW (hashtable_)->Insert (pop_, ikey, reinterpret_cast<Value_t> (ikey));
                if (inserted % 6000000 == 0) {
                    printf ("current ops: %lu ", inserted);
                    D_RW (hashtable_)->checkBufferData ();
                }
            }
            thread->stats.FinishedBatchOp (j);
        }

        thread->stats.real_finish_ = NowMicros ();

        sleep (3);
        if (thread->tid == 0) {
            // double alf = D_RW (hashtable_)->AverageBufLoadFactor();
            // printf("Average Load Factor : %2.2f \n", alf);

            D_RW (hashtable_)->checkBufferData ();
        }
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
        printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
                start_offset + interval);
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
        printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
                start_offset + interval);
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
        printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
                start_offset + interval);
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
        printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
                start_offset + interval);
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
        printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
                start_offset + interval);
        thread->stats.Start ();

        while (key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next ();
                if (thread->ycsb_gen.NextB () == kYCSB_Write) {
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
        printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
                start_offset + interval);
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
        printf ("thread %2d, between %lu - %lu\n", thread->tid,
                (size_t) (start_offset + 0.8 * interval), start_offset + interval);
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
        printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
                start_offset + interval);
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
                if (thread->ycsb_gen.NextG () == kYCSB_Write) {
                    D_RW (hashtable_)->Insert (pop_, key, reinterpret_cast<Value_t> (key));
                    insert++;
                } else {
                    auto ret = D_RW (hashtable_)->Get (key);
                    find++;
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        return;
    }

    void writenwrite (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("write & hybrid key_trace_ initialization.");
            return;
        }
        if (FLAGS_ins_num <= 1) {
            perror ("instance must larger than 1 \n");
            exit (1);
        }

        size_t interval = num_ / (FLAGS_thread);
        size_t start_offset = (thread->tid) * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);
        // printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
        //         start_offset + interval);

        // how many threads should give to each CCEH
        uint32_t nthread = FLAGS_thread / FLAGS_ins_num;  // 8
        Duration duration (FLAGS_readtime, reads_);
        thread->stats.Start ();

        while (!duration.Done (batch) && key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next ();

                uint32_t npolicy = thread->tid / nthread;
                if (npolicy == 0) {
                    D_RW (vhashtable_[npolicy])
                        ->Insert (vpop_[npolicy], key, reinterpret_cast<Value_t> (key));
                } else if (npolicy == 1) {
                    D_RW (vhashtable_[npolicy])
                        ->Insert (vpop_[npolicy], key, reinterpret_cast<Value_t> (key));
                } else {
                    perror ("threads distribute error . \n");
                    exit (1);
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        return;
    }

    void bufferexchange (ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror ("write & hybrid key_trace_ initialization.");
            return;
        }
        if (FLAGS_ins_num <= 1) {
            perror ("instance must larger than 1 \n");
            exit (1);
        }

        size_t interval = num_ / (FLAGS_thread);
        size_t start_offset = (thread->tid) * interval;
        auto key_iterator = key_trace_->iterate_between (start_offset, start_offset + interval);
        // printf ("thread %2d, between %lu - %lu\n", thread->tid, start_offset,
        //         start_offset + interval);

        // how many threads should give to each CCEH
        uint32_t nthread = FLAGS_thread / FLAGS_ins_num;  // 8
        Duration duration (FLAGS_readtime, reads_);
        thread->stats.Start ();

        while (!duration.Done (batch) && key_iterator.Valid ()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid (); j++) {
                size_t key = key_iterator.Next ();

                uint32_t npolicy = thread->tid / nthread;
                if (npolicy == 0) {
                    D_RW (hashtable0_)->Insert (pop0_, key, reinterpret_cast<Value_t> (key));
                } else if (npolicy == 1) {
                    D_RW (hashtable1_)->Insert (pop1_, key, reinterpret_cast<Value_t> (key));
                } else {
                    perror ("threads distribute error . \n");
                    exit (1);
                }
            }
            thread->stats.FinishedBatchOp (j);
        }
        thread->stats.real_finish_ = NowMicros ();
        sleep (10);
        int th = thread->tid;
        if (th == 0) {
            printf ("Node 0: \n ");
            D_RW (hashtable0_)->checkBufferData ();
        }
        if (th == 1) {
            printf ("Node 1: \n ");
            D_RW (hashtable1_)->checkBufferData ();
        }

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
    } else {
        Benchmark benchmark (FLAGS_ins_num);
        benchmark.Run ();
    }
    return 0;
}

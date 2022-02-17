#include "src/buflog.h"

#include "gtest/gtest.h"

using namespace buflog;

// TEST(DataLog, DramCreate) {
//   linkedredolog::DataLog log;
//   EXPECT_FALSE(log.Create(nullptr, 123));
//   EXPECT_TRUE(log.Create(nullptr, 1024));
// }

TEST (BufferLogNode, DramIterator) {
    linkedredolog::BufferLogNode log0;
    linkedredolog::BufferLogNode log1;
    // char *addr = (char *)malloc (1024 * 1024);

    log0.Create (1024 * 1024);
    log1.Create (1024 * 1024);
    LogPtr logPtr0;
    LogPtr logPtr1;

    size_t next0 = 0;
    size_t next1 = 0;

    logPtr0.setData (0, next0);
    logPtr1.setData (1, next1);

    for (uint64_t i = 100; i < 110; i++) {
        uint64_t th = i % 2;

        uint64_t key = i;
        char *val = reinterpret_cast<char *> (i);

        if (th == 0) {
            if (logPtr0.getOffset () == 0) {
                next0 = log0.Append (buflog::kDataLogNodeCheckpoint, key, (uint64_t)val,
                                     logPtr0.getData (), false);
            } else {
                next0 = log0.Append (buflog::kDataLogNodeValid, key, (uint64_t)val,
                                     logPtr0.getData (), false);
            }
            logPtr0.setData (th, next0);

        } else {
            if (logPtr1.getOffset () == 0) {
                next1 = log1.Append (buflog::kDataLogNodeCheckpoint, key, (uint64_t)val,
                                     logPtr1.getOffset (), false);
            } else {
                next1 = log1.Append (buflog::kDataLogNodeValid, key, (uint64_t)val,
                                     logPtr1.getOffset (), false);
            }
            logPtr1.setData (th, next1);
        }
    }
    printf ("log0 data\n");
    char *start = log0.log + 256 + 17;
    uint64_t test = 256 + 17;
    uint64_t k = 0;

    while (k < 10) {
        uint64_t data = *(uint64_t *)(start + 2);
        printf ("test %lu %lu %lu %hhu %hhu %lu \n", *(uint64_t *)(start - 16),
                *(uint64_t *)(start - 8), test, *(uint8_t *)(start), *(uint8_t *)(start + 1),
                *(uint64_t *)(start + 2));
        printf ("data = %lu id = %lu, offset = %lu\n\n", data, data >> 48,
                data & 0x000FFFFFFFFFFFF);

        start = start + 27;
        test += 27;
        k++;
    }

    printf ("log1 data\n");
    char *start1 = log1.log + 256 + 17;
    uint64_t test1 = 256 + 17;
    uint64_t m = 0;

    while (m < 10) {
        uint64_t data = *(uint64_t *)(start1 + 2);
        printf ("test %lu %lu %lu %hhu %hhu %lu \n", *(uint64_t *)(start1 - 16),
                *(uint64_t *)(start1 - 8), test1, *(uint8_t *)(start1), *(uint8_t *)(start1 + 1),
                *(uint64_t *)(start1 + 2));
        printf ("data = %lu id = %lu, offset = %lu\n\n", data, data >> 48,
                data & 0x000FFFFFFFFFFFF);

        start1 = start1 + 27;
        test1 += 27;
        m++;
    }

    printf ("Use the linked list 0 \n");
    auto iter = log0.lBeginRecover (381);
    while (iter.Valid ()) {
        iter.toString ();
        iter++;
    }
    printf ("Use the linked list 1 \n");
    auto iter1 = log1.lBeginRecover (381);
    while (iter1.Valid ()) {
        iter1.toString ();
        iter1++;
    }
}

TEST (BufferLogNode, DramIterator2) {
    linkedredolog::BufferLogNode log0;
    linkedredolog::BufferLogNode log1;
    // char *addr = (char *)malloc (1024 * 1024);

    log0.Create (1024 * 1024);
    log1.Create (1024 * 1024);
    LogPtr logPtr;

    size_t next_ = 0;

    logPtr.setData (0, next_);

    for (uint64_t i = 100; i < 110; i++) {
        uint64_t th = i % 2;

        uint64_t key = i;
        char *val = reinterpret_cast<char *> (i);

        if (th == 0) {
            if (logPtr.getOffset () == 0) {
                next_ = log0.Append (buflog::kDataLogNodeCheckpoint, key, (uint64_t)val,
                                     logPtr.getData (), false);
            } else {
                next_ = log0.Append (buflog::kDataLogNodeValid, key, (uint64_t)val,
                                     logPtr.getData (), false);
            }

        } else {
            if (logPtr.getOffset () == 0) {
                next_ = log1.Append (buflog::kDataLogNodeCheckpoint, key, (uint64_t)val,
                                     logPtr.getOffset (), false);
            } else {
                next_ = log1.Append (buflog::kDataLogNodeValid, key, (uint64_t)val,
                                     logPtr.getOffset (), false);
            }
        }
        logPtr.setData (th, next_);
    }
    printf ("log0 data\n");
    char *start = log0.log + 256 + 17;
    uint64_t test = 256 + 17;
    uint64_t k = 0;

    while (k < 6) {
        uint64_t data = *(uint64_t *)(start + 2);
        printf ("test %lu %lu %lu %hhu %hhu %lu \n", *(uint64_t *)(start - 16),
                *(uint64_t *)(start - 8), test, *(uint8_t *)(start), *(uint8_t *)(start + 1),
                *(uint64_t *)(start + 2));
        printf ("data = %lu id = %lu, offset = %lu\n\n", data, data >> 48,
                data & 0x000FFFFFFFFFFFF);

        start = start + 27;
        test += 27;
        k++;
    }

    printf ("log1 data\n");
    char *start1 = log1.log + 256 + 17;
    uint64_t test1 = 256 + 17;
    uint64_t m = 0;

    while (m < 6) {
        uint64_t data = *(uint64_t *)(start1 + 2);
        printf ("test %lu %lu %lu %hhu %hhu %lu \n", *(uint64_t *)(start1 - 16),
                *(uint64_t *)(start1 - 8), test1, *(uint8_t *)(start1), *(uint8_t *)(start1 + 1),
                *(uint64_t *)(start1 + 2));
        printf ("data = %lu id = %lu, offset = %lu\n\n", data, data >> 48,
                data & 0x000FFFFFFFFFFFF);

        start1 = start1 + 27;
        test1 += 27;
        m++;
    }

    printf ("Use the linked list 0 \n");
    // auto iter = log0.lBeginRecover (381);
    // auto iter = log1.lBeginRecover (381);
    // while (iter.Valid ()) {
    //     iter.toString ();
    //     iter++;
    // }
}

// TEST (BufVec, Operation) {
//     BufVec node;

//     for (int i = 0; i < 7; ++i) {
//         EXPECT_TRUE (node.Insert (random () % 100));
//     }

//     EXPECT_FALSE (node.Insert (8));
//     EXPECT_TRUE (node.CompactInsert (8));
//     node.Sort ();

//     auto iter = node.Begin ();
//     while (iter.Valid ()) {
//         printf ("%lu, ", *iter);
//         ++iter;
//     }

//     printf ("\n");
// }

// TEST (SortedBufNode, PutGet) {
//     SortedBufNode buf_node;

//     char val[128] = "value 123";
//     for (int i = 12; i > 0; i--) {
//         bool res = buf_node.Put (i, val);
//         EXPECT_TRUE (res);
//     }
//     printf ("%s\n", buf_node.ToString ().c_str ());
//     bool res = buf_node.Put (14, val);
//     EXPECT_FALSE (res);
//     buf_node.Sort ();
//     printf ("%s\n", buf_node.ToString ().c_str ());
//     EXPECT_EQ (12, buf_node.ValidCount ());

//     char *tmp = nullptr;
//     for (int i = 12; i > 0; i--) {
//         bool res = buf_node.Get (i, tmp);
//         EXPECT_TRUE (res);
//     }

//     for (int i = 12; i > 0; i--) {
//         bool res = buf_node.Put (i, val);
//         EXPECT_TRUE (res);
//     }

//     tmp = nullptr;
//     for (int i = 12; i > 0; i--) {
//         bool res = buf_node.Get (i, tmp);
//         EXPECT_TRUE (res);
//     }

//     EXPECT_EQ (12, buf_node.ValidCount ());
//     printf ("%s\n", buf_node.ToString ().c_str ());

//     EXPECT_TRUE (buf_node.Delete (10));
//     EXPECT_FALSE (buf_node.Get (10, tmp));
//     EXPECT_FALSE (buf_node.Delete (10));
//     EXPECT_FALSE (buf_node.Get (10, tmp));

//     EXPECT_TRUE (buf_node.Delete (8));
//     EXPECT_FALSE (buf_node.Get (8, tmp));
//     EXPECT_FALSE (buf_node.Delete (8));
//     EXPECT_FALSE (buf_node.Get (8, tmp));

//     EXPECT_TRUE (buf_node.Delete (4));
//     EXPECT_FALSE (buf_node.Delete (4));

//     EXPECT_EQ (9, buf_node.ValidCount ());
//     printf ("%s\n", buf_node.ToString ().c_str ());

//     buf_node.Sort ();
//     printf ("%s\n", buf_node.ToString ().c_str ());

//     buf_node.Put (10, val);
//     buf_node.Sort ();
//     printf ("%s\n", buf_node.ToString ().c_str ());

//     auto iter = buf_node.sBegin ();
//     while (iter.Valid ()) {
//         printf ("%u, ", iter->key);
//         iter++;
//     }
//     printf ("\n");
// }

// TEST (SortedBufNode, MaskLastN) {
//     SortedBufNode buf_node;

//     char val[128] = "value 123";
//     for (int i = 12; i > 0; i--) {
//         bool res = buf_node.Put (i, val);
//         EXPECT_TRUE (res);
//     }

//     buf_node.Sort ();

//     printf ("%s\n", buf_node.ToString ().c_str ());
//     printf ("bufnode before mask: %s\n", buf_node.ToStringValid ().c_str ());

//     buf_node.MaskLastN (5);

//     printf ("bufnode after  mask: %s\n", buf_node.ToStringValid ().c_str ());
//     printf ("%s\n", buf_node.ToString ().c_str ());
// }

// TEST (WriteBuffer, Iterator) {
//     WriteBuffer<8> buf_node;

//     char val[128] = "value 123";
//     for (int i = 40; i > 0; i--) {
//         bool res = buf_node.Put (i, val);
//         EXPECT_TRUE (res);
//     }

//     auto iter = buf_node.Begin ();
//     EXPECT_TRUE (iter.Valid ());

//     int i = 0;
//     while (iter.Valid ()) {
//         printf ("entry %2d: key: %d, val: %s\n", i++, iter->key, iter->val);
//         // printf("%s\n", iter.iter.node_->ToString().c_str());
//         ++iter;
//     }
// }

int main (int argc, char *argv[]) {
    ::testing::InitGoogleTest (&argc, argv);
    return RUN_ALL_TESTS ();
}

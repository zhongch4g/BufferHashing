# sudo ../CCEH-PMDK/ycsb_bench --thread=1 --benchmarks=load,readrandom,readnon  --num=120000000
# num - number of total record
# read - number of read operations

# for nb in 70000 140000
# do
#     # replace the number
#     sed -i '19s/[0-9]*[1-9][0-9]*/'${nb}'/g' /home/zhongchen/CCEH-BUFLOG/CCEH/src/CCEH_BUF.h
#     for factor in 16 8 4 2 1
#     do
#         sed -i '18s/[0-9]*[1-9][0-9]*/'${factor}'/g' /home/zhongchen/CCEH-BUFLOG/CCEH/src/CCEH_BUF.h
#         cd release && make -j32 && cd ..
#         for i in {1..16}
#         do
#             sudo numactl -N 0 release/cceh_bench --thread=$i --benchmarks=load --num=120000000 >> ./result-$((16/$factor))k/limit-buffer-${nb}.log
#         done
#     done
# done

# For no buffer limited version

# for factor in 16 8 4 2 1
# do
#     sed -i '18s/[0-9]*[1-9][0-9]*/'${factor}'/g' /home/zhongchen/CCEH-BUFLOG/CCEH/src/CCEH_BUF.h
#     cd release && make -j32 && cd ..
#     for i in {1..16}
#     do
#         sudo numactl -N 0 release/cceh_bench --thread=$i --benchmarks=load --num=120000000 >> ./result-$((16/$factor))k/unlimit-buffer.log
#     done
# done

# for i in {1..16}
# for i in {6..16}
# do
#     sudo numactl -N 0 release/cceh_bench --thread=$i --benchmarks=load --num=120000000 >> ./result-4k/varies-buffer-0.7.log
# done


# for i in 0 1.0 #  0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9
# do
    # sudo numactl -N 0 ./release/cceh_bench --thread=8 --benchmarks=readall --num=120000000 --stats_interval=1000000 --ins_num=1 --bufferRate=$i >> buffer-impact/rrate$i.log
#     sudo numactl -N 0 ./release/cceh_bench --thread=8 --benchmarks=load --num=120000000 --stats_interval=1000000 --ins_num=1 --bufferRate=$i --bufferNum=1 >> buffer-impact/bufferrate_$i.log
# done

# for i in 0 20000 40000 60000 80000 100000 120000 140000 160000
# do
#     sudo numactl -N 0 ./release/cceh_bench --thread=8 --benchmarks=load --num=120000000 --stats_interval=1000000 --ins_num=1 --bufferRate=0 --bufferNum=$i >> buffer-impact/bufferN_load_$i.log
# done

cd release && make -j32
cd ..
sudo numactl -N 0 ./release/ccehex_bench --thread=16 --benchmarks=loadtest2 --num=120000000 --ins_num=2 --report_interval=1 --batch=10000 >> log.log
# sudo numactl -N 0 ./release/ccehex_bench --thread=8 --benchmarks=loadtest --num=120000000 --ins_num=1 --bufferRate=1 --bufferNum=1 --batch=1000000 >> release/nbufferwrites1.log
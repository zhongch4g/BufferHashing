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

for i in {1..16}
# for i in {6..16}
do
    sudo numactl -N 0 release/cceh_bench --thread=$i --benchmarks=load --num=120000000 >> ./result-4k/varies-buffer-0.7.log
done
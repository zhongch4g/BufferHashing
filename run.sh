# sudo ../CCEH-PMDK/ycsb_bench --thread=1 --benchmarks=load,readrandom,readnon  --num=120000000
# num - number of total record
# read - number of read operations
# write - number of write operations
# sudo ../CCEH-PMDK/ycsb_bench --thread=1 --benchmarks=load,readrandom  --num=120000000

# numactl -N $SOCKET_NO sudo ../CCEH-PMDK/ycsb_bench --thread=$t --benchmarks=load,readrandom,readnon --stats_interval=10000000 --read=10000000 --num=120000000 | tee thread.cceh_$t

# numactl ./cceh_bench --thread=4 --benchmarks=load --buffer=true  --stats_interval=10000000 --num=120000000

# for i in {1..16}
# do
# ./cceh_bench --thread=$i --benchmarks=load --stats_interval=10000000 --num=120000000 | tee ./result-1k/load_regular_th$i.log
# numactl -N 0 release/cceh_bench --thread=$i --benchmarks=load --num=120000000
# sudo numactl -N 0 release/cceh_bench --thread=$i --benchmarks=load,readall --num=120000000 | tee ./result-16k/WR-Only-th$i.log
# sudo numactl -N 0 CCEH/ycsb_bench --thread=$i --benchmarks=load,readall --num=120000000 | tee ./result-CCEH/WR-Only-th$i.log
# sudo numactl -N 0 release/cceh_bench --thread=$i --benchmarks=load,readall --num=123000000 | tee ./result-8k/nometa-noleft-th${i}.log
# done

for num in {110000000..130000000..2000000}
do
    sudo numactl -N 0 release/cceh_bench --thread=16 --benchmarks=load --num=$num >> 8K-num.log
done
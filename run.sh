# sudo ../CCEH-PMDK/ycsb_bench --thread=1 --benchmarks=load,readrandom,readnon  --num=120000000
# num - number of total record
# read - number of read operations
# write - number of write operations

# sudo ../CCEH-PMDK/ycsb_bench --thread=1 --benchmarks=load,readrandom  --num=120000000

# numactl -N $SOCKET_NO sudo ../CCEH-PMDK/ycsb_bench --thread=$t --benchmarks=load,readrandom,readnon --stats_interval=10000000 --read=10000000 --num=120000000 | tee thread.cceh_$t

# numactl ./cceh_bench --thread=4 --benchmarks=load --buffer=true  --stats_interval=10000000 --num=120000000

for i in {1..32}
do
# ./cceh_bench --thread=$i --benchmarks=load --buffer=false  --stats_interval=10000000 --num=120000000 | tee ./result-1k/load_regular_th$i.log
numactl -N 0 ./cceh_bench --thread=$i --benchmarks=load --buffer=true  --stats_interval=10000000 --num=120000000 | tee ./result-1k/load_buffer_th$i.log
done
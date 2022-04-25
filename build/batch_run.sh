for th in 1 2 4 8 16 20 24 28 32 36 40
do
    sudo numactl -N 0 ./cceh_bench --thread=$th --benchmarks=load,readall,readnon --num=120000000 --withlog=true >> bufferhashing.data
done
for th in {1..40}
do
    sudo numactl -N 0 ./ycsb_bench --thread=$th --num=120000000 --benchmarks=load,readrandom >> results/CCEH_CoW_Pos_th$th.txt
    sudo numactl -N 0 ./ycsb_bench --thread=$th --num=120000000 --benchmarks=load,readnon >> results/CCEH_CoW_Neg_th$th.txt
done
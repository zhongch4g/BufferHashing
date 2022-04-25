for th in 1 2 4 8 16 20 24 28 32 36 40
do
    # sudo numactl -N 0 ./ycsb_bench --thread=$th --num=120000000 --benchmarks=load,readall >> results/CCEH_CoW_Pos_th$th.txt
    sudo numactl -N 0 ./ycsb_bench --thread=$th --num=120000000 --benchmarks=load,readall,readnon >> results/CCEH.data
#     sudo numactl -N 0 ./ycsb_bench --thread=$th --num=120000000 --benchmarks=load,readnon >> results/CCEH_CoW_Neg_th$th.txt
done



for th in {1..16}
do
    sudo numactl -N 0 ./ycsb_bench --thread=$th --benchmarks=ycsbb --num=120000000 --batch=10000 --report_interval=1 --gChoice=0 >> results/Origin_CCEH_g0_th$th.txt
done
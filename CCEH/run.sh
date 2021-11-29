sudo numactl -N 0 ./ycsb_bench --thread=4 --benchmarks=ycsbb --num=240000000 --batch=10000 --report_interval=1 --gChoice=0 >> CCEH_g0.txt
sudo numactl -N 0 ./ycsb_bench --thread=4 --benchmarks=ycsbb --num=240000000 --batch=10000 --report_interval=1 --gChoice=1 >> CCEH_g1.txt
sudo numactl -N 0 ./ycsb_bench --thread=4 --benchmarks=ycsbb --num=240000000 --batch=10000 --report_interval=1 --gChoice=2 >> CCEH_g2.txt
sudo numactl -N 0 ./ycsb_bench --thread=4 --benchmarks=ycsbb --num=240000000 --batch=10000 --report_interval=1 --gChoice=3 >> CCEH_g3.txt
sudo numactl -N 0 ./ycsb_bench --thread=4 --benchmarks=ycsbb --num=240000000 --batch=10000 --report_interval=1 --gChoice=4 >> CCEH_g4.txt
sudo numactl -N 0 ./ycsb_bench --thread=4 --benchmarks=ycsbb --num=240000000 --batch=10000 --report_interval=1 --gChoice=5 >> CCEH_g5.txt
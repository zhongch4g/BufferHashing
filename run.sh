
# CCEH-BUFLOG
# sudo numactl -N 0 ./cceh_bench --thread=16 --benchmarks=ycsbb --num=120000000 --report_interval=1 --batch=10000 --gChoice=0

# CCEHEX Fixed-Size CCEH
# sudo numactl -N 0 ./ccehex_bench --thread=40 --benchmarks=cceh2 --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=0 >> results_multi/cceh2_fixed_g0.txt
# sudo numactl -N 0 ./ccehex_bench --thread=40 --benchmarks=cceh2 --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=1 >> results_multi/cceh2_fixed_g1.txt
# sudo numactl -N 0 ./ccehex_bench --thread=40 --benchmarks=cceh2 --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=2 >> results_multi/cceh2_fixed_g2.txt
# sudo numactl -N 0 ./ccehex_bench --thread=40 --benchmarks=cceh2 --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=3 >> results_multi/cceh2_fixed_g3.txt
# sudo numactl -N 0 ./ccehex_bench --thread=40 --benchmarks=cceh2 --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=4 >> results_multi/cceh2_fixed_g4.txt
# sudo numactl -N 0 ./ccehex_bench --thread=40 --benchmarks=cceh2 --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=5 >> results_multi/cceh2_fixed_g5.txt

# CCEH Dynamic 
# sudo numactl -N 0 ./cceh_dual_bench --thread=40 --benchmarks=ccehdual --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=0 >> results_multi/cceh2_dynamic_g0.txt
# sudo numactl -N 0 ./cceh_dual_bench --thread=40 --benchmarks=ccehdual --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=1 >> results_multi/cceh2_dynamic_g1.txt
# sudo numactl -N 0 ./cceh_dual_bench --thread=40 --benchmarks=ccehdual --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=2 >> results_multi/cceh2_dynamic_g2.txt
# sudo numactl -N 0 ./cceh_dual_bench --thread=40 --benchmarks=ccehdual --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=3 >> results_multi/cceh2_dynamic_g3.txt
# sudo numactl -N 0 ./cceh_dual_bench --thread=40 --benchmarks=ccehdual --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=4 >> results_multi/cceh2_dynamic_g4.txt
# sudo numactl -N 0 ./cceh_dual_bench --thread=40 --benchmarks=ccehdual --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=5 >> results_multi/cceh2_dynamic_g5.txt

# Dynamic only consider about write rate
# sudo numactl -N 0 ./cceh_dual_bench --thread=16 --benchmarks=ccehdual2 --num=120000000 --batch=10000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=0 >> results_bufferdist/ccehdual2_g0_16th.log
# sudo numactl -N 0 ./cceh_dual_bench --thread=16 --benchmarks=ccehdual2 --num=120000000 --batch=10000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=5 >> results_bufferdist/ccehdual2_g1_16th.log

# CCEH3 Dynamic 
# sudo numactl -N 0 ./cceh_dual_bench --thread=39 --benchmarks=multicceh --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=3 --bufferNum0=50000 --bufferNum1=50000 --bufferNum2=50000 >> cceh3_dynamic_test.txt
 
# sudo numactl -N 0 ./ccehex_bench --thread=39 --benchmarks=cceh3 --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=3 --bufferNum0=50000 --bufferNum1=50000 --bufferNum2=50000 >> cceh3_fixed_test.txt

# for th in 2 4 8 16 32 64
# do
    # sudo numactl -N 0 ./ccehex_bench --thread=8 --benchmarks=ycsbb --num=120000000 --ins_num=1 --bufferNum=70000 --gChoice=0 --initsize=$th >> results_dirsize/batch_cceh_fixed_g0_dir$th.txt
    # sudo numactl -N 0 ./ccehex_bench --thread=8 --benchmarks=ycsbb --num=120000000 --ins_num=1 --bufferNum=70000 --gChoice=0 --initsize=256 >> results_bufferrate/batch_cceh_fixed_g0_bufferrate$th.txt
# done

# for th in 30000 50000 70000 110000 150000 190000
# do
# sudo numactl -N 0 ./ccehex_bench --thread=8 --benchmarks=ycsbb --num=120000000 --ins_num=1 --bufferNum=$th --gChoice=0 --initsize=256 >> results_bufferrate/batch_cceh_fixed_g0_bufferrate$th.txt
# done

# for th in 0 1 2 3 4 5
# do
# sudo numactl -N 0 ./cceh_dual_bench --thread=16 --benchmarks=ccehdual --num=120000000 --batch=10000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=$th >> results_bufferdist/bufferdist_p5_g$th.txt
# done

# sudo numactl -N 0 ./cceh_dual_bench --thread=20 --benchmarks=ycsbb --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=1 --bufferNum=70000 --gChoice=0 >> results_multi/cceh_fixed_g0.txt
# sudo numactl -N 0 ./cceh_dual_bench --thread=20 --benchmarks=ycsbb --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=1 --bufferNum=70000 --gChoice=1 >> results_multi/cceh_fixed_g1.txt
# sudo numactl -N 0 ./cceh_dual_bench --thread=20 --benchmarks=ycsbb --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=1 --bufferNum=70000 --gChoice=2 >> results_multi/cceh_fixed_g2.txt
# sudo numactl -N 0 ./cceh_dual_bench --thread=20 --benchmarks=ycsbb --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=1 --bufferNum=70000 --gChoice=3 >> results_multi/cceh_fixed_g3.txt
# sudo numactl -N 0 ./cceh_dual_bench --thread=20 --benchmarks=ycsbb --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=1 --bufferNum=70000 --gChoice=4 >> results_multi/cceh_fixed_g4.txt
# sudo numactl -N 0 ./cceh_dual_bench --thread=20 --benchmarks=ycsbb --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=1 --bufferNum=70000 --gChoice=5 >> results_multi/cceh_fixed_g5.txt

# sudo numactl -N 0 ./cceh_bench --thread=8 --benchmarks=ycsbb --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=1 --gChoice=0 --initsize=64 >> results_multi/cceh_buflog_g0.txt
# sudo numactl -N 0 ./cceh_bench --thread=8 --benchmarks=ycsbb --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=1 --gChoice=1 --initsize=64 >> results_multi/cceh_buflog_g1.txt
# sudo numactl -N 0 ./cceh_bench --thread=8 --benchmarks=ycsbb --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=1 --gChoice=2 --initsize=64 >> results_multi/cceh_buflog_g2.txt
# sudo numactl -N 0 ./cceh_bench --thread=8 --benchmarks=ycsbb --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=1 --gChoice=3 --initsize=64 >> results_multi/cceh_buflog_g3.txt
# sudo numactl -N 0 ./cceh_bench --thread=8 --benchmarks=ycsbb --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=1 --gChoice=4 --initsize=64 >> results_multi/cceh_buflog_g4.txt
# sudo numactl -N 0 ./cceh_bench --thread=8 --benchmarks=ycsbb --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=1 --gChoice=5 --initsize=64 >> results_multi/cceh_buflog_g5.txt

sudo numactl -N 0 ./cceh_rec_bench --thread=16 --benchmarks=load --num=60000000 --gChoice=0
# sudo numactl -N 0 ./cceh_rec_bench --thread=16 --benchmarks=load_recovery --num=60000000 --is_recovery=true >> buffer_wal_test.log

for th in {39..40}
do
    # sudo numactl -N 0 ./cceh_bench --thread=$th --num=120000000 --benchmarks=load,readrandom >> ../BufferedCCEH_Improved/CCEH_buflog_Pos_th$th.txt
    # sudo numactl -N 0 ./cceh_bench --thread=$th --num=120000000 --benchmarks=load,readnon >> results/CCEH_buflog_Neg_th$th.txt
done
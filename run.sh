# CCEHEX Fixed-Size CCEH
# sudo numactl -N 0 ./ccehex_bench --thread=40 --benchmarks=cceh2 --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=0 >> results_multi/cceh2_fixed_g0.txt
# sudo numactl -N 0 ./ccehex_bench --thread=40 --benchmarks=cceh2 --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=1 >> results_multi/cceh2_fixed_g1.txt
# sudo numactl -N 0 ./ccehex_bench --thread=40 --benchmarks=cceh2 --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=2 >> results_multi/cceh2_fixed_g2.txt
# sudo numactl -N 0 ./ccehex_bench --thread=40 --benchmarks=cceh2 --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=3 >> results_multi/cceh2_fixed_g3.txt
# sudo numactl -N 0 ./ccehex_bench --thread=40 --benchmarks=cceh2 --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=4 >> results_multi/cceh2_fixed_g4.txt
# sudo numactl -N 0 ./ccehex_bench --thread=40 --benchmarks=cceh2 --num=120000000 --report_interval=0.5 --batch=1000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=5 >> results_multi/cceh2_fixed_g5.txt

# Dynamic only consider about write rate
# sudo numactl -N 0 ./cceh_dual_bench --thread=16 --benchmarks=ccehdual2 --num=120000000 --batch=10000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=0 >> results_bufferdist/ccehdual2_g0_16th.log
# sudo numactl -N 0 ./cceh_dual_bench --thread=16 --benchmarks=ccehdual2 --num=120000000 --batch=10000 --ins_num=2 --bufferNum0=70000 --bufferNum1=70000 --gChoice=5 >> results_bufferdist/ccehdual2_g1_16th.log

# for th in {39..40}
# do
    # sudo numactl -N 0 ./cceh_bench --thread=$th --num=120000000 --benchmarks=load,readrandom >> ../BufferedCCEH_Improved/CCEH_buflog_Pos_th$th.txt
    # sudo numactl -N 0 ./cceh_bench --thread=$th --num=120000000 --benchmarks=load,readnon >> results/CCEH_buflog_Neg_th$th.txt
# done

########################################### WORK PLACE ###########################################
for thread in {1..40}
do
sudo numactl -N 0 ./cceh_bench --thread=$thread --benchmarks=load,readall --num=120000000 --report_interval=1 --batch=10000 >> results/CCEH_buflog_CoW_positive_read_th$thread.txt
sudo numactl -N 0 ./cceh_bench --thread=$thread --benchmarks=load,readnon --num=120000000 --report_interval=1 --batch=10000 >> results/CCEH_buflog_CoW_negtive_read_th$thread.txt
done


########################################### WORK PLACE ###########################################


########################################### CCEH-BUFLOG ##########################################
# For LOAD without Log
# sudo numactl -N 0 ./cceh_bench --thread=16 --benchmarks=load --num=120000000 --report_interval=1 --batch=10000

# For LOAD with Log
# sudo numactl -N 0 ./cceh_bench --thread=16 --benchmarks=load --num=120000000 --withlog=true --report_interval=1 --batch=10000

# For Positive Read
# sudo numactl -N 0 ./cceh_bench --thread=16 --benchmarks=load,readall --num=120000000 --report_interval=1 --batch=10000

# For Negtive Read
# sudo numactl -N 0 ./cceh_bench --thread=16 --benchmarks=load,readnon --num=120000000 --report_interval=1 --batch=10000

# Set Write Ratio
# sudo numactl -N 0 ./cceh_bench --thread=16 --benchmarks=ycsbb --num=120000000 --report_interval=1 --batch=10000 --gChoice=0

# Recovery Test
# sudo numactl -N 0 ./cceh_bench --thread=16 --benchmarks=load --num=60000000 --withlog=true --report_interval=1 --batch=10000
# sudo numactl -N 0 ./cceh_bench --thread=16 --benchmarks=recovery --recovery=true --report_interval=1 --batch=10000

# Combine the limit buffer 
########################################### CCEH-BUFLOG ##########################################

######################################## CCEH-BUFLOG-LIMIT #######################################
# Single instance without Log
# sudo numactl -N 0 ./cceh_limit_bench --thread=16 --benchmarks=load --num=120000000 --bufferNum=70000 --ins_num=1

# Single instance with Log
sudo numactl -N 0 ./cceh_limit_bench --thread=16 --benchmarks=load --num=120000000 --bufferNum=70000 --ins_num=1 --withlog=true

# Two instances without log
sudo numactl -N 0 ./cceh_limit_bench --thread=16 --benchmarks=load --num=120000000 --bufferNum0=70000 --bufferNum1=70000 --ins_num=2


######################################## CCEH-BUFLOG-LIMIT #######################################


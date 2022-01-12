"""
For 2 DIMMs results
"""
import pandas as pd
import numpy as np

def get_dual_results(path, nparts, time_weight):
    buffer_distribution = []
    thread_epoch = []
    # Read the data from the file
    with open(path, "r") as f:
        for line in f.readlines():
            if (line.startswith("TIME|")):
                thread_epoch.append(line.strip("TIME|").strip("\n").split(","))
            # else:
            #     buffer_distribution.append(line.strip("\n").split(","))

    # if (buffer_distribution):
    #     buffer_distribution = np.array(buffer_distribution)
    #     buffer_distribution = pd.DataFrame(buffer_distribution, columns=['cceh0', 'cceh1', 'total'], dtype=np.float)

    if (thread_epoch):
        thread_epoch = np.array(thread_epoch)
        thread_epoch = pd.DataFrame(thread_epoch, columns=['thread', 'last_done', 'cur_done', 'seg_time', 'time'], dtype=np.float64)
    
    # Find the MAX thread 
    max_thread = int(thread_epoch["thread"].max()) + 1
    thread_step = int(max_thread / nparts)

    """
    Calculate throughput : (epoch_ops/1/1024/1024)
    """
    max_execute_time = 0
    min_execute_time = 9999
    CCEH_max_execute_time = dict()
    for i in range(nparts): # Loop N CCEH
        start, end = i * thread_step, thread_step * (i + 1)
        local_execute_time = 0

        for j in range(start, end):
            execute_time = thread_epoch[thread_epoch["thread"]==j].shape[0]
            if (execute_time > local_execute_time):
                local_execute_time = execute_time
        CCEH_max_execute_time[i] = local_execute_time
        
        if (local_execute_time < min_execute_time):
            min_execute_time = local_execute_time

        if (local_execute_time > max_execute_time):
            max_execute_time = local_execute_time
        print("thread ", start, " to ", end-1, "CCEH ", i, " runtime = ", local_execute_time)
        
    print ("All CCEH min runtime :", min_execute_time * time_weight, "All CCEH max runtime :", max_execute_time * time_weight)

    CCEH_Mops = dict()
    CCEH_time = dict()
    CCEH_throughput = dict()
    for i in range(nparts): # Loop N CCEH
        start, end = i * thread_step, thread_step * (i + 1)
        CCEH_Mops[i] = np.zeros(min_execute_time)
        CCEH_time[i] = np.array([k for k in np.arange(time_weight, min_execute_time*time_weight + time_weight, time_weight)])
        for j in range(start, end):
            thread_info = thread_epoch[thread_epoch["thread"]==j]
            epoch = thread_info['cur_done'] - thread_info['last_done']
            padding = np.pad(epoch.values, (0, CCEH_max_execute_time[i] - epoch.shape[0]), 'constant', constant_values=(0,0))
            CCEH_Mops[i] += padding[:min_execute_time]/1024/1024
        CCEH_throughput[i] = CCEH_Mops[i] / time_weight
        print("CCEH Throughput ", i, " -> ", sum(CCEH_Mops[i]), "/", min_execute_time*time_weight, sum(CCEH_Mops[i])/(min_execute_time*time_weight))

    overall_Mops = 0
    # Calculate overall throughput
    for idx in range(nparts):
        overall_Mops += sum(CCEH_Mops[idx])
    print ("Overall throughput = ", overall_Mops / (min_execute_time*time_weight))
    return CCEH_time, CCEH_throughput


time_d, throughput_d = get_dual_results("../release/cceh_dynamic_g2.txt", 2, 0.5)
time_f, throughput_f = get_dual_results("../release/cceh_fixed_g2.txt", 2, 0.5)
# time_d, throughput_d = get_dual_results("../release/cceh3_dynamic_test.txt", 3, 0.5)
# time_f, throughput_f = get_dual_results("../release/cceh3_fixed_test.txt", 3, 0.5)

import matplotlib.pyplot as plt
import sys
from matplotlib.ticker import (MultipleLocator, FormatStrFormatter,AutoMinorLocator)
from matplotlib.ticker import FuncFormatter
import matplotlib
# plt.rcParams["font.family"] = "arial"
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

gloyend = None
# Main
dashes=[(2,2), (4,1), (2,0), (2,0), (3, 3), (2, 0), (2,2), (4,1), (2,0), (2,0), (3, 3), (2, 0)]
markers = ['x', '|', '.', 'D', 'd', '', 'x', '|', '.', 'D', 'd', '']
colors = ['grey', '#FF7F0E', '#2077B4', '#D62728', '#0A640C', '#343434', 'grey', '#FF7F0E', '#2077B4', '#D62728', '#0A640C', '#343434']
# colors = ['#2077B4', '#D62728', '#0A640C', '#343434']

label = ['Dynamic-CCEH0', 'Dynamic-CCEH1', 'Dynamic-TOTAL', 'Fixed-CCEH0', 'Fixed-CCEH1', 'Fixed-TOTAL']

# d0 d1 represents dynamic CCEH data
# f0 f1 represents fixed CCEH data

fig, ax = plt.subplots(2, 2, figsize=(16, 7.2), constrained_layout=True, sharex=True, sharey=True)

ax[0,0].plot(time_d[0], throughput_d[0], color=colors[0], marker=markers[0], dashes=dashes[0], label = label[0], alpha=0.8, fillstyle='none', markersize=4)
ax[0,0].plot(time_d[1], throughput_d[1], color=colors[1], marker=markers[1], dashes=dashes[1], label = label[1], alpha=0.8, fillstyle='none', markersize=4)

if (throughput_d[0].shape[0] > throughput_d[1].shape[0]):
    thr1 = np.pad(throughput_d[1], (0,throughput_d[0].shape[0] - throughput_d[1].shape[0]), 'constant', constant_values=(0,0))
    ax[0,0].plot(time_d[0], thr1+throughput_d[0], color=colors[2], marker=markers[2], dashes=dashes[2], label = label[2], alpha=0.8, fillstyle='none', markersize=4)
else:
    thr0 = np.pad(throughput_d[0], (0,throughput_d[1].shape[0] - throughput_d[0].shape[0]), 'constant', constant_values=(0,0))
    ax[0,0].plot(time_d[1], thr0+throughput_d[1], color=colors[2], marker=markers[2], dashes=dashes[2], label = label[2], alpha=0.8, fillstyle='none', markersize=4)

ax[0,0].plot(time_f[0], throughput_f[0], color=colors[3], marker=markers[3], dashes=dashes[3], label = label[3], alpha=0.8, fillstyle='none', markersize=4)
ax[0,0].plot(time_f[1], throughput_f[1], color=colors[4], marker=markers[4], dashes=dashes[4], label = label[4], alpha=0.8, fillstyle='none', markersize=4)

if (throughput_f[0].shape[0] > throughput_f[1].shape[0]):
    thrb = np.pad(throughput_f[1], (0,throughput_f[0].shape[0] - throughput_f[1].shape[0]), 'constant', constant_values=(0,0))
    ax[0,0].plot(time_f[0], thrb+throughput_f[0], color=colors[5], marker=markers[5], dashes=dashes[5], label = label[5], alpha=0.8, fillstyle='none', markersize=4)
else:
    thra = np.pad(throughput_f[0], (0,throughput_f[1].shape[0] - throughput_f[0].shape[0]), 'constant', constant_values=(0,0))
    ax[0,0].plot(time_f[1], thra+throughput_f[1], color=colors[5], marker=markers[5], dashes=dashes[5], label = label[5], alpha=0.8, fillstyle='none', markersize=4)

ax[0,0].legend(loc="upper right")
ax[0,0].grid(which='major', linestyle='--', zorder=0)
ax[0,0].grid(which='minor', linestyle='--', zorder=0, linewidth=0.3)
ax[0,0].xaxis.grid(False, which='both')
# ax[0,0].set_title('Time Epoch(1 secs) Throughput \n CCEH0 70% Write CCEH1 100% Write', fontsize = 14)
# ax[0,0].set_xlabel('Time Epoch (secs)', fontsize=12)


if (throughput_d[0].shape[0] > throughput_d[1].shape[0]):
    thr1 = np.pad(throughput_d[1], (0,throughput_d[0].shape[0] - throughput_d[1].shape[0]), 'constant', constant_values=(0,0))
    ax[0,1].plot(time_d[0], thr1+throughput_d[0], color=colors[2], marker=markers[2], dashes=dashes[2], label = label[2], alpha=0.8, fillstyle='none', markersize=4)
else:
    thr0 = np.pad(throughput_d[0], (0,throughput_d[1].shape[0] - throughput_d[0].shape[0]), 'constant', constant_values=(0,0))
    ax[0,1].plot(time_d[1], thr0+throughput_d[1], color=colors[2], marker=markers[2], dashes=dashes[2], label = label[2], alpha=0.8, fillstyle='none', markersize=4)

if (throughput_f[0].shape[0] > throughput_f[1].shape[0]):
    thrb = np.pad(throughput_f[1], (0,throughput_f[0].shape[0] - throughput_f[1].shape[0]), 'constant', constant_values=(0,0))
    ax[0,1].plot(time_f[0], thrb+throughput_f[0], color=colors[5], marker=markers[5], dashes=dashes[5], label = label[5], alpha=0.8, fillstyle='none', markersize=4)
else:
    thra = np.pad(throughput_f[0], (0,throughput_f[1].shape[0] - throughput_f[0].shape[0]), 'constant', constant_values=(0,0))
    ax[0,1].plot(time_f[1], thra+throughput_f[1], color=colors[5], marker=markers[5], dashes=dashes[5], label = label[5], alpha=0.8, fillstyle='none', markersize=4)

ax[0,1].legend(loc="upper right")
ax[0,1].grid(which='major', linestyle='--', zorder=0)
ax[0,1].grid(which='minor', linestyle='--', zorder=0, linewidth=0.3)
ax[0,1].xaxis.grid(False, which='both')
# ax[0,1].set_title('Time Epoch(1 secs) Throughput \n CCEH0 70% Write CCEH1 100% Write', fontsize = 14)
# ax[0,1].set_xlabel('Time Epoch (secs)', fontsize=12)
# ax[0,0].set_ylabel('Throughput (Mops/s)', fontsize=12)


# 1 0
ax[1,0].plot(time_d[0], throughput_d[0], color=colors[0], marker=markers[0], dashes=dashes[0], label = label[0], alpha=0.8, fillstyle='none', markersize=4)
ax[1,0].plot(time_f[0], throughput_f[0], color=colors[3], marker=markers[3], dashes=dashes[3], label = label[3], alpha=0.8, fillstyle='none', markersize=4)

# ax[1,0].plot(time_g0, through_g0, color=colors[4], marker=markers[3], dashes=dashes[3], label = label[3], alpha=0.8, fillstyle='none', markersize=4)
# ax[1,0].plot(time_t0, through_t0, color=colors[5], marker=markers[3], dashes=dashes[3], label = label[3], alpha=0.8, fillstyle='none', markersize=4)
# ax[1,0].plot(time_t1, through_t1, color=colors[1], marker=markers[3], dashes=dashes[3], label = label[3], alpha=0.8, fillstyle='none', markersize=4)

ax[1,0].legend(loc="upper right")
ax[1,0].grid(which='major', linestyle='--', zorder=0)
ax[1,0].grid(which='minor', linestyle='--', zorder=0, linewidth=0.3)
ax[1,0].xaxis.grid(False, which='both')

# 1 1
ax[1,1].plot(time_d[1], throughput_d[1], color=colors[1], marker=markers[1], dashes=dashes[1], label = label[1], alpha=0.8, fillstyle='none', markersize=4)
ax[1,1].plot(time_f[1], throughput_f[1], color=colors[4], marker=markers[4], dashes=dashes[4], label = label[4], alpha=0.8, fillstyle='none', markersize=4)
# ax[1,1].plot(time_cmp6[:20], through_cmp6[:20], color=colors[6], marker=markers[4], dashes=dashes[4], label = 'CCEH-BUFLOG', alpha=0.8, fillstyle='none', markersize=4)

ax[1,1].legend(loc="upper right")
ax[1,1].grid(which='major', linestyle='--', zorder=0)
ax[1,1].grid(which='minor', linestyle='--', zorder=0, linewidth=0.3)
ax[1,1].xaxis.grid(False, which='both')


fig.suptitle(f'Time Epoch(1 secs) Throughput \n CCEH0 60% Write CCEH1 100% Write (Buffer-70K)', fontsize = 14)
fig.supxlabel('Time Epoch (secs)', fontsize=12)
fig.supylabel('Throughput (Mops/s)', fontsize=12)

fig.savefig("./results/cceh_comparison/cceh_comparison_g2.pdf", bbox_inches='tight', pad_inches=0)
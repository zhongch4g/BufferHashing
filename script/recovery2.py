"""
For 2 DIMMs results
"""
import pandas as pd
import numpy as np

def get_dual_results(path):
    buffer_dist = []
    statistic = []
    with open(path, "r") as f:
        for line in f.readlines():
            if (line.startswith("TIME|")):
                statistic.append(line.strip("TIME|").strip("\n").split(","))
            else:
                buffer_dist.append(line.strip("\n").split(","))
#     if (buffer_dist):
#         buffer_dist = np.array(buffer_dist)
#         buffer_dist = pd.DataFrame(buffer_dist, columns=['cceh0', 'cceh1', 'total'], dtype=np.float)

    if (statistic):
        statistic = np.array(statistic)
        statistic = pd.DataFrame(statistic, columns=['thread', 'last_done', 'cur_done', 'seg_time', 'time'], dtype=np.float)
    
    """
    Calculate throughput : (epoch_ops/epoch_time/1024/1024)
    """
    max_size0 = 0
    max_size1 = 0
    for i in range(16):
        th = statistic[statistic["thread"]==i].shape
        if (th[0] > max_size0):
            max_size0 = th[0]
    
    MAX_SIZE = max_size0

    cceh0 = np.zeros(max_size0)
    
    for i in range(16):
        th = statistic[statistic["thread"]==i]
        cur_done = th['cur_done'].values
        last_done = th['last_done'].values
        eopch = cur_done - last_done
        cur_length = cur_done.shape[0]
        padding = np.pad(eopch, (0, max_size0-cur_length), 'constant', constant_values=(0,0))
        cceh0 += padding
    cceh0_Mops = cceh0[:MAX_SIZE]/0.02/1024/1024
    time_x = [i for i in np.arange(0.02, (MAX_SIZE + 1)*0.02, 0.02)]
    through_x = cceh0_Mops
    print("Through0: ", sum(through_x), "/", time_x[-1], sum(through_x)/time_x[-1])

    return time_x, through_x


# recovery comparison
time_rec0, through_rec0 = get_dual_results("./Recovery/normal_read2.txt")
time_frec0, through_frec0 = get_dual_results("./Recovery/fast_rec_read2.txt")


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
colors = ['grey', '#FF7F0E', '#2077B4', '#D62728', '#0A640C', '#343434', '#008837']
# colors = ['#2077B4', '#D62728', '#0A640C', '#343434']

label = ['Non-Warmup', 'Warmup']

# d0 d1 represents dynamic CCEH data
# f0 f1 represents fixed CCEH data

fig, ax = plt.subplots(figsize=(4, 3.6), constrained_layout=True, sharex=True, sharey=True)

# print(time_rec0, through_rec0)
t = [i for i in np.arange(0, time_rec0[-1] + 2.73, 0.02)]
print(len(t))
print(len(t) - len(through_rec0))

thr = []
for i in range(len(t) - len(through_rec0)):
    thr.append(0)

for i in through_rec0:
    thr.append(i)

node = 170

ax.plot(time_frec0[1:node], through_frec0[1:node], color=colors[5], marker=markers[2], dashes=dashes[2], label = label[1], alpha=0.8, fillstyle='none', markersize=10)
ax.plot(t[1:node], thr[1:node], color=colors[3], marker=markers[2], dashes=dashes[2], label = label[0], alpha=0.8, fillstyle='none', markersize=10)

ystart, yend = ax.get_ylim()
ax.set_ylim([0.00001, yend*1.09])
xstart, xend = ax.get_xlim()
ax.set_xlim([-xend/8, xend])

ax.legend(loc='upper center', ncol=2, borderaxespad=0.3, frameon=False)
ax.tick_params(axis="y",direction="in", pad=-21, labelsize=12)
ax.xaxis.grid(False, which='both')
# ax.set_xticks(np.arange(1, 18, 5))
ax.set_yticks(np.arange(1, 18, 5))
# ax[0,1].set_title('Time Epoch(1 secs) Throughput \n CCEH0 70% Write CCEH1 100% Write', fontsize = 14)
ax.set_xlabel('Time Epoch (secs)', fontsize=12)
ax.set_ylabel('Throughput (Mops/s)', fontsize=12)


# fig.suptitle(f'CCEH0 0% Write CCEH1 100% Write (Buffer-70K)', fontsize = 14)
# fig.supxlabel('Time Epoch (secs)', fontsize=18)
# fig.supylabel('Throughput (Mops/s)', fontsize=18)

fig.savefig("./Recovery_Warmup.pdf", bbox_inches='tight', pad_inches=0)
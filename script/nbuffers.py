from cProfile import label
import matplotlib.pyplot as plt
import sys
import numpy as np
from matplotlib.ticker import (MultipleLocator, FormatStrFormatter,AutoMinorLocator)
from matplotlib.ticker import FuncFormatter
import matplotlib
# plt.rcParams["font.family"] = "arial"
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

BH_OPS = []
BH_TM = []
BH_TP = []
BH_R = []
BH_W = []
with open("../release/BH_WAL_20.data", "r") as f:
    for iter in f.readlines():
        if ("*operations*" in iter):
            BH_OPS.append(float(iter.split()[-1]))
        if ("*real_elapsed*" in iter):
            BH_TM.append(float(iter.split()[-1]))
        if ("*throughput*" in iter):
            BH_TP.append(float(iter.split()[-1]))
        if ("*DIMM-R*" in iter):
            BH_R.append(float(iter.strip().strip("MB").split()[-1]))
        if ("*DIMM-W*" in iter):
            BH_W.append(float(iter.strip().strip("MB").split()[-1]))

print("Buffer Hashing", len(BH_OPS), len(BH_TM), len(BH_TP), len(BH_R), len(BH_W))

BH_Write_Th = np.array(BH_TP)
BH_Write_R_IO = np.array(BH_R)
BH_Write_W_IO = np.array(BH_W)
BH_Write_BW = (BH_Write_W_IO + BH_Write_R_IO) /np.array(BH_TM)


percentage = ["0%", "20%", "40%", "60%", "80%", "100%"]
index = np.array([0, 1, 2, 3, 4, 5])
fig, ax = plt.subplots(figsize=(4, 3.6), constrained_layout=True)

ax.plot(percentage, BH_Write_Th, label="BHT Throughput", marker='*', color="#606060", fillstyle='none', markersize=12, zorder=1)
# ax.plot(percentage, BH_Write_BW, label="BH bandwidth", marker='^', color="#0A640C", fillstyle='none', markersize=10)
# ax.plot(Thread, DASH_Write_Th, label="DASH", marker='o', color="#2077B4", fillstyle='none', markersize=10)

ax.set_ylabel('Throughput (Mops/s)', fontsize=14)
ax.set_ylim([0.1, max(BH_Write_Th)*1.1])
ax.tick_params(axis="y",direction="in", pad=-20, labelsize=12)

ystart, yend = ax.get_ylim()
ax.set_ylim([0.1, yend*1.09])
xstart, xend = ax.get_xlim()
ax.set_xlim([-xend/8, xend])

ax.tick_params(axis="x", labelsize=12)
ax.set_xlabel('Buffer Ratio', fontsize=14)

ax.legend(loc='upper left', ncol=1, borderaxespad=0.3, frameon=False)

ax2=ax.twinx()
width = 0.25
ax2.bar(index-width/2, BH_Write_W_IO/1024, color = '#a881bc',
        width = width, edgecolor = 'black',
        label='Write IO',zorder = 2)
ax2.bar(index+width/2, BH_Write_R_IO/1024, color = '#91aacf',
        width = width, edgecolor = 'black',
        label='Read IO',zorder = 3)

plt.xticks(index, percentage, rotation=45)

ax2.set_ylabel("Pmem IO (GB)", fontsize=14)
ax2.tick_params(axis="y",direction="in", pad=-30, labelsize=12)

ax2.yaxis.set_ticks(np.arange(0, 150, 50))

ystart, yend = ax2.get_ylim()
ax2.set_ylim([1, yend*1.7])
xstart, xend = ax.get_xlim()
ax2.set_xlim([-xend/5, xend*1.1])
ax2.legend(loc='upper right', ncol=1, borderaxespad=0.3, frameon=False)


ax.set_zorder(ax2.get_zorder()+1)
ax.patch.set_visible(False)



fig.savefig("evaluation/bh_with_n_buffers.pdf", bbox_inches='tight', pad_inches=0)
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

CCEH_OPS = []
CCEH_TM = []
CCEH_TP = []
CCEH_R = []
CCEH_W = []
with open("../CCEH/results/CCEH.data", "r") as f:
    for iter in f.readlines():
        if ("*operations*" in iter):
            CCEH_OPS.append(float(iter.split()[-1]))
        if ("*real_elapsed*" in iter):
            CCEH_TM.append(float(iter.split()[-1]))
        if ("*throughput*" in iter):
            CCEH_TP.append(float(iter.split()[-1]))
        if ("*DIMM-R*" in iter):
            CCEH_R.append(float(iter.strip().strip("MB").split()[-1]))
        if ("*DIMM-W*" in iter):
            CCEH_W.append(float(iter.strip().strip("MB").split()[-1]))

print("CCEH", len(CCEH_OPS), len(CCEH_TM), len(CCEH_TP), len(CCEH_R), len(CCEH_W))

CCEH_Write_Th = CCEH_TP[0::3]
CCEH_Read_Th = CCEH_TP[1::3]
CCEH_Write_IO = np.array(CCEH_W[0::3]) + np.array(CCEH_R[0::3])
CCEH_Read_IO = np.array(CCEH_W[1::3]) + np.array(CCEH_R[1::3])
CCEH_Write_BW = CCEH_Write_IO /np.array(CCEH_TM[0::3])
CCEH_Read_BW = CCEH_Read_IO /np.array(CCEH_TM[1::3])

BH_OPS = []
BH_TM = []
BH_TP = []
BH_R = []
BH_W = []
with open("../release/bufferhashing.data", "r") as f:
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

BH_Write_Th = BH_TP[0::3]
BH_Read_Th = BH_TP[1::3]
BH_Write_IO = np.array(BH_W[0::3]) + np.array(BH_R[0::3])
BH_Read_IO = np.array(BH_W[1::3]) + np.array(BH_R[1::3])
BH_Write_BW = BH_Write_IO /np.array(BH_TM[0::3])
BH_Read_BW = BH_Read_IO /np.array(BH_TM[1::3])


DASH_OPS = []
DASH_TM = []
DASH_TP = []
DASH_R = []
DASH_W = []
with open("../../Dash/release/results/DASH.data", "r") as f:
    for iter in f.readlines():
        if ("*operations*" in iter):
            DASH_OPS.append(float(iter.split()[-1]))
        if ("*real_elapsed*" in iter):
            DASH_TM.append(float(iter.split()[-1]))
        if ("*throughput*" in iter):
            DASH_TP.append(float(iter.split()[-1]))
        if ("*DIMM-R*" in iter):
            DASH_R.append(float(iter.strip().strip("MB").split()[-1]))
        if ("*DIMM-W*" in iter):
            DASH_W.append(float(iter.strip().strip("MB").split()[-1]))

print("DASH", len(DASH_OPS), len(DASH_TM), len(DASH_TP), len(DASH_R), len(DASH_W))

DASH_Write_Th = DASH_TP[0::3]
DASH_Read_Th = DASH_TP[1::3]
DASH_Write_IO = np.array(DASH_W[0::3]) + np.array(DASH_R[0::3])
DASH_Read_IO = np.array(DASH_W[1::3]) + np.array(DASH_R[1::3])
DASH_Write_BW = DASH_Write_IO /np.array(DASH_TM[0::3])
DASH_Read_BW = DASH_Read_IO /np.array(DASH_TM[1::3])



args_len = len(sys.argv)
if (args_len != 2):
    print ("usage: \n"+
    "1 write_throughput\n"+
    "2 write_IO\n"+
    "3 write_bandwidth\n"+
    "4 read_throughput\n"+
    "5 read_IO\n"+
    "6 read_bandwidth")
    exit()


"""
1. write throughput
2. write IO
3. write bandwidth
4. read throughput
5. read IO
6. read bandwidth
"""
tmap = {"1":"write_throughput", "2":"write_IO", "3":"write_bandwidth", \
    "4":"read_throughput", "5":"read_IO", "6":"read_bandwidth"}
gtype = sys.argv[1]

Thread = [1, 2, 4, 8, 16, 20, 24, 28, 32, 36, 40]

fig, ax = plt.subplots(figsize=(4, 3.6), constrained_layout=True)

if (gtype == "1"):
    ax.plot(Thread, BH_Write_Th, label="BH", marker='*', color="#D62728", fillstyle='none', markersize=12)
    ax.plot(Thread, CCEH_Write_Th, label="CCEH", marker='^', color="#0A640C", fillstyle='none', markersize=10)
    ax.plot(Thread, DASH_Write_Th, label="DASH", marker='o', color="#2077B4", fillstyle='none', markersize=10)
    ax.set_ylabel('Throughput (Mops/s)', fontsize=14)
    ax.set_ylim([0.1, max(BH_Write_Th)*1.1])
    ax.tick_params(axis="y",direction="in", pad=-20, labelsize=12)

if (gtype == "2"):
    ax.plot(Thread, DASH_Write_IO/1024, label="DASH", marker='o', color="#2077B4", fillstyle='none', markersize=10)
    ax.plot(Thread, CCEH_Write_IO/1024, label="CCEH", marker='^', color="#0A640C", fillstyle='none', markersize=10)
    ax.plot(Thread, BH_Write_IO/1024, label="BH", marker='*', color="#D62728", fillstyle='none', markersize=12)
    ax.set_ylabel('Pmem I/O (GB)', fontsize=14)
    ax.tick_params(axis="y",direction="in", pad=-28, labelsize=12)

if (gtype == "3"):
    ax.plot(Thread, CCEH_Write_BW/1024, label="CCEH", marker='^', color="#0A640C", fillstyle='none', markersize=10)
    ax.plot(Thread, BH_Write_BW/1024, label="BH", marker='*', color="#D62728", fillstyle='none', markersize=12)
    ax.plot(Thread, DASH_Write_BW/1024, label="DASH", marker='o', color="#2077B4", fillstyle='none', markersize=10)
    ax.set_ylabel('Pmem Bandwidth (GB/s)', fontsize=14)

if (gtype == "4"):
    ax.plot(Thread, BH_Read_Th, label="BH", marker='*', color="#D62728", fillstyle='none', markersize=12)
    ax.plot(Thread, CCEH_Read_Th, label="CCEH", marker='^', color="#0A640C", fillstyle='none', markersize=10)
    ax.plot(Thread, DASH_Read_Th, label="DASH", marker='o', color="#2077B4", fillstyle='none', markersize=10)
    ax.set_ylabel('Throughput (Mops/s)', fontsize=14)
    ax.set_ylim([0.1, max(BH_Read_Th)*1.1])
    ax.tick_params(axis="y",direction="in", pad=-20, labelsize=12)

if (gtype == "5"):
    ax.plot(Thread, DASH_Read_IO/1024, label="DASH", marker='o', color="#2077B4", fillstyle='none', markersize=10)
    ax.plot(Thread, CCEH_Read_IO/1024, label="CCEH", marker='^', color="#0A640C", fillstyle='none', markersize=10)
    ax.plot(Thread, BH_Read_IO/1024, label="BH", marker='*', color="#D62728", fillstyle='none', markersize=12)
    ax.set_ylabel('Pmem I/O (GB)', fontsize=14)
    ax.tick_params(axis="y",direction="in", pad=-28, labelsize=12)

if (gtype == "6"):
    ax.plot(Thread, CCEH_Read_BW/1024, label="CCEH", marker='^', color="#0A640C", fillstyle='none', markersize=10)
    ax.plot(Thread, BH_Read_BW/1024, label="BH", marker='*', color="#D62728", fillstyle='none', markersize=12)
    ax.plot(Thread, DASH_Read_BW/1024, label="DASH", marker='o', color="#2077B4", fillstyle='none', markersize=10)
    ax.set_ylabel('Pmem Bandwidth (GB/s)', fontsize=14)

ystart, yend = ax.get_ylim()
ax.set_ylim([0.1, yend*1.09])
xstart, xend = ax.get_xlim()
ax.set_xlim([-xend/8, xend])

ax.tick_params(axis="x", labelsize=12)
ax.set_xlabel('# of Threads', fontsize=14)

ax.legend(loc='upper center', ncol=3, borderaxespad=0.3, frameon=False)
fig.savefig("evaluation/" + tmap[gtype] + "_slogging.pdf", bbox_inches='tight', pad_inches=0)
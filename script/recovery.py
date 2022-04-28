import matplotlib.pyplot as plt
import sys
import numpy as np
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

label = ['SCAN', 'BHT WAL']

# d0 d1 represents dynamic CCEH data
# f0 f1 represents fixed CCEH data

fig, ax = plt.subplots(figsize=(4, 3.6), constrained_layout=True, sharex=True, sharey=True)

t = [i for i in range(1,17)]
r1 = [0.007015, 0.010317, 0.006994, 0.007034, 0.007095,0.0071,0.006954,0.007373,0.00706,0.0071,0.00706,0.00706,0.006939,0.007036,0.007136,0.00718]

r3 = [0.26,0.13,0.09,0.07,0.05,0.05,0.04,0.04,0.03,0.03,0.03,0.03,0.03,0.02,0.02,0.02]
ax.plot(t, r3, color=colors[5], marker=markers[5], dashes=dashes[5], label = label[0], alpha=0.8, fillstyle='none', markersize=12)
ax.plot(t, r1, color=colors[3], marker=markers[2], dashes=dashes[2], label = label[1], alpha=0.8, fillstyle='none', markersize=12)

# ax.set_ylim([0.00001, max(r3)*1.25])
ystart, yend = ax.get_ylim()
ax.set_ylim([0.00001, yend*1.09])
xstart, xend = ax.get_xlim()
ax.set_xlim([-xend/8, xend])

ax.legend(loc='upper center', ncol=2, borderaxespad=0.3, frameon=False)
ax.tick_params(axis="y",direction="in", pad=-32, labelsize=12)
ax.set_xticks(np.arange(1, 17, 3))

# ax[0,1].set_title('Time Epoch(1 secs) Throughput \n CCEH0 70% Write CCEH1 100% Write', fontsize = 14)
ax.set_xlabel('# of Threads', fontsize=12)
ax.set_ylabel('Recovery Time (sec)', fontsize=12)


# fig.suptitle(f'CCEH0 0% Write CCEH1 100% Write (Buffer-70K)', fontsize = 14)
# fig.supxlabel('Time Epoch (secs)', fontsize=18)
# fig.supylabel('Throughput (Mops/s)', fontsize=18)

fig.savefig("recovery.pdf", bbox_inches='tight', pad_inches=0)
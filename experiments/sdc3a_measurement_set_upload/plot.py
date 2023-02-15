import datetime

from matplotlib.colors import LogNorm
from matplotlib.patches import ConnectionPatch, Rectangle
import pandas as pd
import numpy as np
import pylab as plt
import seaborn as sns

sns.set_style("whitegrid")

fts_timestamp_format = '%Y-%m-%dT%H:%M:%SZ'
rucio_timestamp_format = '%Y-%m-%d %H:%M:%S'

fts_timestamps = []
throughputs = []
with open("fts.csv", 'r') as f:
    for idx, line in enumerate(f):
        try:
            if idx==0:
                continue
            timestamp, explanation, decision, running, queue, success_rate, throughput, ema, diff = line.split(',')
            diff = diff.rstrip('\n')
            
            if 'MiB/s' in throughput:
                parsed_throughput = float(throughput.replace('MiB/s', '').strip())
            if 'KiB/s' in throughput:
                parsed_throughput = float(throughput.replace('KiB/s', '').strip())/1E3
            if 'bytes/s' in throughput:
                parsed_throughput = float(throughput.replace('bytes/s', '').strip())/1E6
            #if np.isclose(parsed_throughput, 0):# or parsed_throughput<0:
            #    parsed_throughput = np.nan

            fts_timestamps.append(datetime.datetime.strptime(timestamp, fts_timestamp_format))
            throughputs.append(parsed_throughput)
        except ValueError:
            continue

#plt.plot(timestamps, throughputs, '-')
#plt.fill_between(timestamps, throughputs, step="pre", alpha=0.4)

rucio_timestamps = []
submitteds = []
submission_faileds = []
queueds = []
faileds = []
dones = []
with open("rucio.csv", 'r') as f:
   for idx, line in enumerate(f):
        try:
            if idx==0:
                continue
            timestamp, submitted, submission_failed, queued, failed, done = line.split(',')
            done = done.rstrip('\n')

            rucio_timestamps.append(datetime.datetime.strptime(timestamp, rucio_timestamp_format))
            submitteds.append(int(submitted))
            submission_faileds.append(int(submission_failed))
            queueds.append(int(queued))
            faileds.append(int(failed))
            dones.append(int(done))
        except ValueError:
            continue
submitteds = np.array(submitteds)
submission_faileds = np.array(submission_faileds)
queueds = np.array(queueds)
faileds = np.array(faileds)
dones = np.array(dones)

# rucio
## ingest rucio data as pd df
rucio_df = pd.DataFrame({
    'submitted': submitteds,
    'done': dones,
    'failed': faileds,
    'queued': queueds
}, index=rucio_timestamps)

#rucio_df = pd.DataFrame({
#    'successful': dones/submitteds,
#    'failed': faileds/submitteds,
#}, index=rucio_timestamps)

## groupby some frequency
#rucio_df = rucio_df.groupby(pd.Grouper(freq='180T')).sum()
rucio_df = rucio_df.groupby(pd.Grouper(freq='60T')).mean()

hours_between_columns = [int((p1-p0).total_seconds()/3600) for p1, p0 in zip(rucio_df.index[1:], rucio_df.index[0:-1])]
hours_after_transfer = np.cumsum([0] + hours_between_columns)
rucio_df = rucio_df.set_index(hours_after_transfer)

## transpose for heatmap
rucio_df = rucio_df.transpose()

## replace 0s with some arbitrarily small number (for lognorm plot)
rucio_df = rucio_df.replace(to_replace=0, value=1E-6)
print(rucio_df)

# FTS
## ingest fts data as pd df
fts_df = pd.DataFrame({
    'throughputs': throughputs
}, index=fts_timestamps)

## groupby some frequency
max_throughputs = fts_df.groupby(pd.Grouper(freq='10T')).max().rename(columns={'throughputs': 'max_throughputs'})
min_throughputs = fts_df.groupby(pd.Grouper(freq='10T')).min().rename(columns={'throughputs': 'min_throughputs'})
av_throughputs = fts_df.groupby(pd.Grouper(freq='10T')).mean().rename(columns={'throughputs': 'av_throughputs'})

fts_df = pd.concat([min_throughputs, max_throughputs, av_throughputs], axis=1, join="inner")

hours_between_columns = [float((p1-p0).total_seconds()/3600.) for p1, p0 in zip(fts_df.index[1:], fts_df.index[0:-1])]
hours_after_transfer = np.cumsum([0] + hours_between_columns)
fts_df = fts_df.set_index(hours_after_transfer)

fts_df = fts_df.replace(np.nan, 0)
print(fts_df)

# create plots
fix, ax = plt.subplots(2, 2, gridspec_kw={'width_ratios': [100,5]})
#ax[1, 1].remove()
sns.heatmap(rucio_df, norm=LogNorm(vmin=1), cmap=sns.color_palette("Reds", as_cmap=True), xticklabels=8, ax=ax[0, 0], cbar_ax=ax[0, 1], cbar_kws={"format": '%.e'}) # counts
#sns.heatmap(rucio_df, vmin=0, vmax=100, cmap=sns.color_palette("mako", as_cmap=True), xticklabels=8, ax=ax[0, 0], cbar_ax=ax[0, 1]) # %
ax[0, 0].set_ylabel("Event type")
ax[0, 0].set_xlim((0, None))
ax[0, 0].set_xticks([])
ax[0, 0].set_xticks([], minor=True)

sns.lineplot(data=fts_df, x=fts_df.index, y="av_throughputs", linewidth=1, ax=ax[1, 0])
#sns.lineplot(data=fts_df, x=fts_df.index, y="min_throughputs", linewidth=1, ax=ax[1, 0])
ax[1, 0].set_xlim((0, None))
ax[1, 0].set_ylim((0, None))
ax[1, 0].set_ylabel("Throughput (MiB/s)")
ax[1, 0].set_xlabel("Hours since start of transfer")

sns.histplot(data=fts_df.replace(0, np.nan), y="av_throughputs", element="step", kde=True, fill=True, ax=ax[1, 1])
ax[1, 1].set_xlabel("")
ax[1, 1].set_ylabel("")
ax[1, 1].set_xticks([])
ax[1, 1].set_xticks([], minor=True)
ax[1, 1].set_yticks([])
ax[1, 1].set_yticks([], minor=True)

# add cell highlights
#hm_ax.add_patch(Rectangle((3, 0), 10, 1, fill=False, edgecolor='blue', lw=2))


plt.tight_layout()
plt.show()


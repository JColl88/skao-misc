import datetime

import numpy as np
import pylab as plt

timestamp_format = '%Y-%m-%dT%H:%M:%SZ'

timestamps = []
throughputs = []
with open("out.csv", 'r') as f:
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
            if np.isclose(parsed_throughput, 0) or parsed_throughput<1:
                parsed_throughput = np.nan

            timestamps.append(datetime.datetime.strptime(timestamp, timestamp_format))
            throughputs.append(parsed_throughput)
        except ValueError:
            continue

plt.plot(timestamps, throughputs, '-')
plt.fill_between(timestamps, throughputs, step="pre", alpha=0.4)
plt.show()



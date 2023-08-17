import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.gridspec as gridspec

# Load data
lag_data = pd.read_csv('sen_avg_lag_normal.csv')
throughput_data = pd.read_csv('sen_avg_throughput_topic_normal.csv')
overhead_throughput_data = pd.read_csv('sen_avg_overhead_throughput_topic_normal.csv')

# Adjust file names and merge datasets
lag_data = lag_data.rename(columns={'FileName': 'File'})
lag_data['File'] = lag_data['File'].str.replace('_consumer_count.csv', '_message_throughput.csv')
merged_data = pd.merge(lag_data, throughput_data, on='File')
overhead_throughput_data['File'] = overhead_throughput_data['File'].str.replace('_overhead_throughput.csv', '_message_throughput.csv')
merged_data_3d = pd.merge(merged_data, overhead_throughput_data, on='File', suffixes=('', '_overhead'))

# Compute 3D Pareto front
def pareto_front_3d(data):
    data = data.sort_values(by=['avg_lag', 'Average Throughput', 'Average Throughput_overhead'], ascending=[True, False, True])
    pareto_data = []
    max_throughput = float("-inf")
    min_overhead = float("inf")
    for _, row in data.iterrows():
        if row['Average Throughput'] >= max_throughput and row['Average Throughput_overhead'] <= min_overhead:
            pareto_data.append(row)
            max_throughput = row['Average Throughput']
            min_overhead = row['Average Throughput_overhead']
    return pd.DataFrame(pareto_data)
pareto_data_3d = pareto_front_3d(merged_data_3d)

print(pareto_data_3d)

dummy_text_data_3d = {
    "sen_1_message_throughput.csv": "1",
    "sen_4_message_throughput.csv": "4",
    "sen_10_message_throughput.csv": "10",
    "sen_12_message_throughput.csv": "12",
    "sen_9_message_throughput.csv": "9",
    "sen_15_message_throughput.csv": "15",
}

# remove the pareto data from the merged data
merged_data_3d = merged_data_3d[~merged_data_3d['File'].isin(pareto_data_3d['File'])]

# 3D visualization
fig = plt.figure(figsize=(14, 12))
ax = fig.add_subplot(111, projection='3d')
ax.set_xlim([0.90, 0.97])
ax.set_ylim([0.5, 2.2])
ax.set_zlim([3, 10])
ax.scatter(pareto_data_3d['Average Throughput'], pareto_data_3d['avg_lag'], pareto_data_3d['Average Throughput_overhead'],
           color='red', s=150, edgecolors='k', depthshade=False, label='Pareto Front', zorder=10)
ax.scatter(merged_data_3d['Average Throughput'], merged_data_3d['avg_lag'], merged_data_3d['Average Throughput_overhead'],
           color='blue', s=50, alpha=0.5, label='All Data Points', zorder=0, marker='o')

# Draw lines from Pareto front points to the axes (within the limits)
for _, row in pareto_data_3d.iterrows():
    ax.plot([row['Average Throughput'], row['Average Throughput']], [row['avg_lag'], row['avg_lag']], [row['Average Throughput_overhead'], ax.get_zlim()[0]], color='grey', linestyle='--', linewidth=0.8)
    ax.plot([row['Average Throughput'], row['Average Throughput']], [row['avg_lag'], ax.get_ylim()[0]], [ax.get_zlim()[0], ax.get_zlim()[0]], color='grey', linestyle='--', linewidth=0.8)
    ax.plot([row['Average Throughput'], ax.get_xlim()[1]], [row['avg_lag'], row['avg_lag']], [ax.get_zlim()[0], ax.get_zlim()[0]], color='grey', linestyle='--', linewidth=0.8)



ax.plot_trisurf(pareto_data_3d['Average Throughput'], pareto_data_3d['avg_lag'], pareto_data_3d['Average Throughput_overhead'],
                color='red', alpha=0.3, shade=False, linewidth=0.1, zorder=5)

for _, row in pareto_data_3d.iterrows():
    ax.text(row['Average Throughput'], row['avg_lag']-0.06, row['Average Throughput_overhead'],
             dummy_text_data_3d[row['File']], fontsize=12, ha='right', va='top', color='k', fontweight='bold', zorder=10, bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

ax.set_xlabel('Average Throughput (messages/second)', labelpad=14)
ax.set_ylabel('Inconsistency (seconds)', labelpad=14)
ax.set_zlabel('Average Overhead Throughput (Dyconit messages/second)', labelpad=16)

ax.legend(loc='upper left', fontsize=10)
ax.view_init(elev=20, azim=-70)
plt.tight_layout()

# Save figure
plt.savefig(f'../e5/pareto3d_normal.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)
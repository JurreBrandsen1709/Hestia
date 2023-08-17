import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

# Load the data
lag_data = pd.read_csv('sen_avg_lag_normal.csv')
throughput_data = pd.read_csv('sen_avg_throughput_topic_normal.csv')

# Rename columns and merge data
lag_data = lag_data.rename(columns={'FileName': 'File'})
lag_data['File'] = lag_data['File'].str.replace('_consumer_count.csv', '_message_throughput.csv')
merged_data = pd.merge(lag_data, throughput_data, on='File')

# Compute Pareto front
def pareto_front(data):
    data = data.sort_values(by=['avg_lag', 'Average Throughput'], ascending=[True, False])
    pareto_data = []
    max_throughput = float("-inf")
    for _, row in data.iterrows():
        if row['Average Throughput'] >= max_throughput:
            pareto_data.append(row)
            max_throughput = row['Average Throughput']
    return pd.DataFrame(pareto_data)

pareto_data = pareto_front(merged_data)
print(pareto_data)

# Dummy data for the table rows
dummy_data = [
    ["15", "1.2", "+8, -4"],
    ["9", "0.8", "+4, -2"],
    ["2", "1.0", "+1, -.5"],
    ["4", "1.4", "+1, -.5"],
    ["1", "0.8", "+1, -.5"]
]

dummy_text_data = {
    "sen_1_message_throughput.csv": "1",
    "sen_4_message_throughput.csv": "4",
    "sen_2_message_throughput.csv": "2",
    "sen_9_message_throughput.csv": "9",
    "sen_15_message_throughput.csv": "15"
}

# Setup figure and grid
fig = plt.figure(figsize=(10, 6))
gs = gridspec.GridSpec(1, 2, width_ratios=[2, 1])
ax1 = plt.subplot(gs[0])
ax2 = plt.subplot(gs[1], frame_on=False)  # Table axis

# Plot on ax1
ax1.scatter(merged_data['Average Throughput'], merged_data['avg_lag'], color='blue', label='All Data Points', marker='x')
ax1.scatter(pareto_data['Average Throughput'], pareto_data['avg_lag'], color='red', label='Pareto Front', marker='o', s=50)
ax1.plot(pareto_data['Average Throughput'], pareto_data['avg_lag'], color='red', linestyle='-')
for _, row in pareto_data.iterrows():
    ax1.plot([1.0, row['Average Throughput']], [row['avg_lag'], row['avg_lag']], lw=1, color='gray', linestyle=':')
    ax1.plot([row['Average Throughput'], row['Average Throughput']], [0.5, row['avg_lag']], lw=1, color='gray', linestyle=':')
    ax1.text(row['Average Throughput'], row['avg_lag'], dummy_text_data[row['File']], ha='right', va='top', fontsize=12, rotation=30, fontweight='bold')

ax1.set_xlim(0.965, 0.9)
ax1.set_ylim(0.5, 2.25)
ax1.set_xlabel('Average Throughput (messages/second)')
ax1.set_ylabel('Inconsistency (seconds)')
ax1.legend()

# Display table with dummy data on ax2
dummy_data.insert(0, ["ID", "Threshold", "Rule"])  # Add headers
ax2.axis('off')
ax2.table(cellText=dummy_data, cellLoc='center', loc='center', colWidths=[0.2, 0.3, 0.2])

plt.tight_layout()
plt.savefig(f'../e5/pareto_normal.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)
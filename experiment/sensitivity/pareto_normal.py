import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns

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

dummy_text_data = {
    "sen_1_message_throughput.csv": "1",
    "sen_4_message_throughput.csv": "4",
    "sen_2_message_throughput.csv": "2",
    "sen_9_message_throughput.csv": "9",
    "sen_15_message_throughput.csv": "15"
}

# Setup figure and grid
sns.set_context("notebook")
sns.set_style("whitegrid")
fig = plt.figure(figsize=(10, 6))
ax1 = fig.add_subplot(1, 1, 1)

# Plot on ax1
ax1.scatter(merged_data['Average Throughput'], merged_data['avg_lag'], color='blue', label='All Data Points', marker='x')
ax1.scatter(pareto_data['Average Throughput'], pareto_data['avg_lag'], color='red', label='Pareto Front', marker='o', s=50)
ax1.plot(pareto_data['Average Throughput'], pareto_data['avg_lag'], color='red', linestyle='-')
for _, row in pareto_data.iterrows():
    ax1.plot([1.0, row['Average Throughput']], [row['avg_lag'], row['avg_lag']], lw=1, color='gray', linestyle=':')
    ax1.plot([row['Average Throughput'], row['Average Throughput']], [0.0, row['avg_lag']], lw=1, color='gray', linestyle=':')
    ax1.text(row['Average Throughput'], row['avg_lag'], dummy_text_data[row['File']], ha='right', va='top', fontsize=12, rotation=30, fontweight='bold')

ax1.set_xlim(0.965, 0.9)
ax1.set_ylim(0.0, 2.50)
ax1.set_xlabel('Average Throughput [messages/second]')
ax1.set_ylabel('Inconsistency [seconds]')
ax1.legend()

plt.tight_layout()
plt.savefig(f'../e5/pareto_normal.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)

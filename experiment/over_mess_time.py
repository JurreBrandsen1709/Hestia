import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV files
df0_m = pd.read_csv('docker/p0_message_throughput.csv')
df1_m = pd.read_csv('docker/p1_message_throughput.csv')
df2_m = pd.read_csv('docker/p2_message_throughput.csv')
df3_m = pd.read_csv('docker/p3_message_throughput.csv')

# Convert the 'Time' column to datetime format and seconds
df0_m['Time'] = (pd.to_datetime(df0_m['Time']) - pd.to_datetime(df0_m['Time']).min()).dt.total_seconds()
df1_m['Time'] = (pd.to_datetime(df1_m['Time']) - pd.to_datetime(df1_m['Time']).min()).dt.total_seconds()
df2_m['Time'] = (pd.to_datetime(df2_m['Time']) - pd.to_datetime(df2_m['Time']).min()).dt.total_seconds()
df3_m['Time'] = (pd.to_datetime(df3_m['Time']) - pd.to_datetime(df3_m['Time']).min()).dt.total_seconds()


# Remove all throughputs of 0
df0_m = df0_m[df0_m['Throughput'] != 0]
df1_m = df1_m[df1_m['Throughput'] != 0]
df2_m = df2_m[df2_m['Throughput'] != 0]
df3_m = df3_m[df3_m['Throughput'] != 0]

# Separate the dataframes based on the 'Topic' column
df0_normal_m = df0_m[df0_m['Topic'] == 'topic_normal']
df0_priority_m = df0_m[df0_m['Topic'] == 'topic_priority']

df1_normal_m = df1_m[df1_m['Topic'] == 'topic_normal']
df1_priority_m = df1_m[df1_m['Topic'] == 'topic_priority']

df2_normal_m = df2_m[df2_m['Topic'] == 'topic_normal']
df2_priority_m = df2_m[df2_m['Topic'] == 'topic_priority']

df3_normal_m = df3_m[df3_m['Topic'] == 'topic_normal']
df3_priority_m = df3_m[df3_m['Topic'] == 'topic_priority']

# Normalize all dataframes to the same elapsed time
max_time_normal_m = min(df0_normal_m['Time'].max(), df1_normal_m['Time'].max(), df2_normal_m['Time'].max(), df3_normal_m['Time'].max())
max_time_priority_m = min(df0_priority_m['Time'].max(), df1_priority_m['Time'].max(), df2_priority_m['Time'].max(), df3_priority_m['Time'].max())

df0_normal_m = df0_normal_m[df0_normal_m['Time'] <= max_time_normal_m]
df1_normal_m = df1_normal_m[df1_normal_m['Time'] <= max_time_normal_m]
df2_normal_m = df2_normal_m[df2_normal_m['Time'] <= max_time_normal_m]
df3_normal_m = df3_normal_m[df3_normal_m['Time'] <= max_time_normal_m]

df0_priority_m = df0_priority_m[df0_priority_m['Time'] <= max_time_priority_m]
df1_priority_m = df1_priority_m[df1_priority_m['Time'] <= max_time_priority_m]
df2_priority_m = df2_priority_m[df2_priority_m['Time'] <= max_time_priority_m]
df3_priority_m = df3_priority_m[df3_priority_m['Time'] <= max_time_priority_m]

# Calculate the average throughput over the different FileId's
df0_normal_avg_m = df0_normal_m.groupby('Time').Throughput.mean().reset_index()
df1_normal_avg_m = df1_normal_m.groupby('Time').Throughput.mean().reset_index()
df2_normal_avg_m = df2_normal_m.groupby('Time').Throughput.mean().reset_index()
df3_normal_avg_m = df3_normal_m.groupby('Time').Throughput.mean().reset_index()

df0_priority_avg_m = df0_priority_m.groupby('Time').Throughput.mean().reset_index()
df1_priority_avg_m = df1_priority_m.groupby('Time').Throughput.mean().reset_index()
df2_priority_avg_m = df2_priority_m.groupby('Time').Throughput.mean().reset_index()
df3_priority_avg_m = df3_priority_m.groupby('Time').Throughput.mean().reset_index()

# Load the CSV files
df1 = pd.read_csv('docker/p1_overhead_throughput.csv')
df2 = pd.read_csv('docker/p2_overhead_throughput.csv')
df3 = pd.read_csv('docker/p3_overhead_throughput.csv')

# Convert the 'Time' column to datetime format and seconds
df1['Time'] = (pd.to_datetime(df1['Time']) - pd.to_datetime(df1['Time']).min()).dt.total_seconds()
df2['Time'] = (pd.to_datetime(df2['Time']) - pd.to_datetime(df2['Time']).min()).dt.total_seconds()
df3['Time'] = (pd.to_datetime(df3['Time']) - pd.to_datetime(df3['Time']).min()).dt.total_seconds()

# Remove all throughputs of 0
df1 = df1[df1['Throughput'] != 0]
df2 = df2[df2['Throughput'] != 0]
df3 = df3[df3['Throughput'] != 0]


df1_normal = df1[df1['Topic'] == 'topic_normal']
df1_priority = df1[df1['Topic'] == 'topic_priority']

df2_normal = df2[df2['Topic'] == 'topic_normal']
df2_priority = df2[df2['Topic'] == 'topic_priority']

df3_normal = df3[df3['Topic'] == 'topic_normal']
df3_priority = df3[df3['Topic'] == 'topic_priority']

# Normalize all dataframes to the same elapsed time
max_time_normal = min(df1_normal['Time'].max(), df2_normal['Time'].max(), df3_normal['Time'].max())
max_time_priority = min(df1_priority['Time'].max(), df2_priority['Time'].max(), df3_priority['Time'].max())


df1_normal = df1_normal[df1_normal['Time'] <= max_time_normal]
df2_normal = df2_normal[df2_normal['Time'] <= max_time_normal]
df3_normal = df3_normal[df3_normal['Time'] <= max_time_normal]

df1_priority = df1_priority[df1_priority['Time'] <= max_time_priority]
df2_priority = df2_priority[df2_priority['Time'] <= max_time_priority]
df3_priority = df3_priority[df3_priority['Time'] <= max_time_priority]

# Calculate the average throughput over the different FileId's
df1_normal_avg = df1_normal.groupby('Time').Throughput.mean().reset_index()
df2_normal_avg = df2_normal.groupby('Time').Throughput.mean().reset_index()
df3_normal_avg = df3_normal.groupby('Time').Throughput.mean().reset_index()

df1_priority_avg = df1_priority.groupby('Time').Throughput.mean().reset_index()
df2_priority_avg = df2_priority.groupby('Time').Throughput.mean().reset_index()
df3_priority_avg = df3_priority.groupby('Time').Throughput.mean().reset_index()

# Create the subplots
plt.figure(figsize=(12, 8))

colors = ['blue', 'orange', 'green', 'red']
labels = ['No Sync', 'Reverse TCP', 'Moving Average', 'Exponential Smoothing']

plt.subplot(2, 1, 1)
plt.plot(df0_priority_avg_m['Time'], df0_priority_avg_m['Throughput'], color=colors[0], label=labels[0])
plt.plot(df1_priority_avg_m['Time'], df1_priority_avg_m['Throughput'], color=colors[1], label=labels[1])
plt.plot(df2_priority_avg_m['Time'], df2_priority_avg_m['Throughput'], color=colors[2], label=labels[2])
plt.plot(df3_priority_avg_m['Time'], df3_priority_avg_m['Throughput'], color=colors[3], label=labels[3])
plt.xlabel('Time (seconds)')
plt.ylabel('Average Throughput')
plt.title('Average Message Throughput over Time for Topic Priority')
plt.grid()
plt.legend()

plt.subplot(2, 1, 2)
plt.plot(df1_priority_avg['Time'], df1_priority_avg['Throughput'], color=colors[1], label=labels[1])
plt.plot(df2_priority_avg['Time'], df2_priority_avg['Throughput'], color=colors[2], label=labels[2])
plt.plot(df3_priority_avg['Time'], df3_priority_avg['Throughput'], color=colors[3], label=labels[3])
plt.xlabel('Time (seconds)')
plt.ylabel('Average Throughput')
plt.ylim(bottom=0)
plt.title('Average Overhead Throughput over Time for Topic Priority')
plt.legend()

plt.grid()
plt.tight_layout()
plt.savefig('docker/e1_w2_prio_time.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)


# Create the subplots
plt.figure(figsize=(12, 8))

colors = ['blue', 'orange', 'green', 'red']
labels = ['No Sync', 'Reverse TCP', 'Moving Average', 'Exponential Smoothing']

plt.subplot(2, 1, 1)
plt.plot(df0_normal_avg_m['Time'], df0_normal_avg_m['Throughput'], color=colors[0], label=labels[0])
plt.plot(df1_normal_avg_m['Time'], df1_normal_avg_m['Throughput'], color=colors[1], label=labels[1])
plt.plot(df2_normal_avg_m['Time'], df2_normal_avg_m['Throughput'], color=colors[2], label=labels[2])
plt.plot(df3_normal_avg_m['Time'], df3_normal_avg_m['Throughput'], color=colors[3], label=labels[3])
plt.xlabel('Time (seconds)')
plt.ylabel('Average Throughput')
plt.title('Average Message Throughput over Time for Topic Normal')
plt.grid()
plt.legend()

plt.subplot(2, 1, 2)
plt.plot(df1_normal_avg['Time'], df1_normal_avg['Throughput'], color=colors[1], label=labels[1])
plt.plot(df2_normal_avg['Time'], df2_normal_avg['Throughput'], color=colors[2], label=labels[2])
plt.plot(df3_normal_avg['Time'], df3_normal_avg['Throughput'], color=colors[3], label=labels[3])
plt.xlabel('Time (seconds)')
plt.ylabel('Average Throughput')
plt.title('Average Overhead Throughput over Time for Topic Normal')
plt.grid()
plt.ylim(bottom=0)
plt.legend()

plt.tight_layout()
plt.savefig('docker/e1_w2_normal_time.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)
import pandas as pd
import matplotlib.pyplot as plt

def process_data(file_name):
    df = pd.read_csv(file_name)
    df['Time'] = (pd.to_datetime(df['Time']) - pd.to_datetime(df['Time']).min()).dt.total_seconds()
    df = df[df['Throughput'] != 0]

    df_normal = df[df['Topic'] == 'topic_normal']
    df_priority = df[df['Topic'] == 'topic_priority']

    return df_normal, df_priority

def normalize_and_avg(df_normal, df_priority, max_time_normal, max_time_priority):
    df_normal = df_normal[df_normal['Time'] <= max_time_normal]
    df_priority = df_priority[df_priority['Time'] <= max_time_priority]

    df_normal_avg = df_normal.groupby('Time').Throughput.mean().reset_index()
    df_priority_avg = df_priority.groupby('Time').Throughput.mean().reset_index()

    return df_normal_avg, df_priority_avg

def plot_data(df_normal_avg, df_priority_avg, color, label, title):
    plt.plot(df_normal_avg['Time'], df_normal_avg['Throughput'], color=color, label=label)
    plt.plot(df_priority_avg['Time'], df_priority_avg['Throughput'], color=color, label=label)
    plt.xlabel('Time (seconds)')
    plt.ylabel('Average Throughput')
    plt.title(title)
    plt.grid()
    plt.legend()

# Load and process the data
file_names = ['docker/p0_message_throughput.csv', 'docker/p1_message_throughput.csv', 'docker/p2_message_throughput.csv', 'docker/p3_message_throughput.csv']
dfs_normal = []
dfs_priority = []
for file_name in file_names:
    df_normal, df_priority = process_data(file_name)
    dfs_normal.append(df_normal)
    dfs_priority.append(df_priority)

# Normalize and calculate average
max_time_normal = min(df['Time'].max() for df in dfs_normal)
max_time_priority = min(df['Time'].max() for df in dfs_priority)
for i in range(len(file_names)):
    dfs_normal[i], dfs_priority[i] = normalize_and_avg(dfs_normal[i], dfs_priority[i], max_time_normal, max_time_priority)

# Plot the data
colors = ['blue', 'orange', 'green', 'red']
labels = ['No Sync', 'Reverse TCP', 'Moving Average', 'Exponential Smoothing']
for i in range(len(file_names)):
    plot_data(dfs_normal[i], dfs_priority[i], colors[i], labels[i], 'Average Message Throughput over Time')

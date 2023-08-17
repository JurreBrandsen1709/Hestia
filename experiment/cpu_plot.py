import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def process_df(path):
    # Reading the CSV file
    df1 = pd.read_csv(path)

    # Converting the "Time" column to total seconds since the beginning
    df1['Time'] = (pd.to_datetime(df1['Time']) - pd.to_datetime(df1['Time']).min()).dt.total_seconds()

    # Grouping the data by 60-second intervals and taking the mean of the "Utilization" column
    df1['Time_interval'] = (df1['Time'] // 60) * 60
    average_utilization_per_60s = df1.groupby('Time_interval')['Utilization'].mean()

    return average_utilization_per_60s

workloads = ['w1', 'w2', 'w3']
colors = sns.color_palette()

# for workload in workloads:

    # Define the file paths and corresponding configurations
paths_configs = [
    (f'star_topology/s_w1_p1_cpu_utilization.csv', 'Undersaturated Workload', 'Undersaturated Workload'),
    (f'star_topology/s_w2_p1_cpu_utilization.csv', 'Fluctuating Workload', 'Fluctuating Workload'),
    (f'star_topology/s_w3_p1_cpu_utilization.csv', 'Oversaturated Workload', 'Oversaturated Workload'),
    ]

# Create a dictionary to hold all the data
all_data = {}

for path, config_priority, config_normal in paths_configs:
    average_utilization_per_60s = process_df(path)
    all_data[config_normal] = average_utilization_per_60s

# retrieve the shortest time interval
# shortest_time_interval = min([data.index[-1] for data in all_data.values()])

# Plotting the line graph
plt.figure(figsize=(14, 6))
for i, (config, data) in enumerate(all_data.items()):
    data[:].plot(label=config, color=colors[i])
plt.xlabel('Time (seconds)')
plt.ylabel('Average Utilization (%)')
plt.legend()  # Add a legend to label the lines
plt.grid(True)
plt.tight_layout()
plt.savefig(f'e3/s_w1-3_p1_cpu_utilization.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)

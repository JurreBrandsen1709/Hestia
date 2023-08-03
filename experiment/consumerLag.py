import pandas as pd
import matplotlib.pyplot as plt

def process_file(file_path):
    """
    Load the CSV file into a DataFrame, process the data, and return the computed average Consumer Lag
    for each ConsumerCount.
    """
    # Load the data from the CSV file
    data = pd.read_csv(file_path)

    # Separate the data into two dataframes based on the Topic
    data_priority = data[data['Topic'] == 'topic_priority']
    data_normal = data[data['Topic'] == 'topic_normal']

    # Convert the Time column from string to datetime
    data_priority['Time'] = pd.to_datetime(data_priority['Time'])
    data_normal['Time'] = pd.to_datetime(data_normal['Time'])

    # Sort the data by ConsumerCount and Time
    data_priority = data_priority.sort_values(['ConsumerCount', 'Time'])
    data_normal = data_normal.sort_values(['ConsumerCount', 'Time'])

    # Calculate the Consumer Lag between consecutive rows
    data_priority['TimeDifference'] = data_priority.groupby('ConsumerCount')['Time'].diff()
    data_normal['TimeDifference'] = data_normal.groupby('ConsumerCount')['Time'].diff()

    # Convert the Consumer Lag to seconds for easier analysis
    data_priority['TimeDifference'] = data_priority['TimeDifference'].dt.total_seconds()
    data_normal['TimeDifference'] = data_normal['TimeDifference'].dt.total_seconds()

    # Calculate the maximum Consumer Lag for each ConsumerCount for each Topic
    max_time_diff_priority = data_priority.groupby('ConsumerCount')['TimeDifference'].max()
    max_time_diff_normal = data_normal.groupby('ConsumerCount')['TimeDifference'].max()

    # Calculate the average Consumer Lag for each ConsumerCount for each Topic
    # average_time_diff_priority = data_priority.groupby('ConsumerCount')['TimeDifference'].mean()
    # average_time_diff_normal = data_normal.groupby('ConsumerCount')['TimeDifference'].mean()

    return max_time_diff_priority, max_time_diff_normal

# List of file paths
file_paths = [
    'star_topology/s_w1_p1_consumer_count.csv',
    'star_topology/s_w1_p2_consumer_count.csv',
    'star_topology/s_w1_p3_consumer_count.csv',
]

# Dictionary to store average Consumer Lags for each file
average_time_diffs = {}

# Process each file
for file_path in file_paths:
    average_time_diffs[file_path.split('/')[-1]] = process_file(file_path)

# Define labels for each file name
file_labels = {
    's_w1_p1_consumer_count.csv': 'Dyconits Enabled - TCP Policy',
    's_w1_p2_consumer_count.csv': 'Dyconits Enabled - Moving Average Policy',
    's_w1_p3_consumer_count.csv': 'Dyconits Enabled - Exponential Smoothing Policy',
}

# Figure for 'topic_priority'
plt.figure(figsize=(10, 6))
for file_name, (average_time_diff_priority, _) in average_time_diffs.items():
    label = file_labels.get(file_name, file_name)  # Get the label from the dictionary, or use the file name as the label
    plt.plot(average_time_diff_priority.index, average_time_diff_priority.values, label=label)
plt.xlabel('Consumer count')
plt.ylabel('Maximum Consumer Lag (seconds)')
plt.legend()
plt.grid(True)
plt.savefig('star_topology/s_w1_lag_priority.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)
# plt.show()

# Figure for 'topic_normal'
plt.figure(figsize=(10, 6))
for file_name, (_, average_time_diff_normal) in average_time_diffs.items():
    label = file_labels.get(file_name, file_name)  # Get the label from the dictionary, or use the file name as the label
    plt.plot(average_time_diff_normal.index, average_time_diff_normal.values, label=label)
plt.xlabel('ConsumerCount')
plt.ylabel('Maximum Consumer Lag (seconds)')
plt.legend()
plt.grid(True)
plt.savefig('star_topology/s_w1_lag_normal.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)
# plt.show()

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from itertools import cycle

def process_file(file_path, topic1, topic2):
    """
    Load the CSV file into a DataFrame, process the data, and return the computed average Consumer Lag
    for each ConsumerCount.
    """
    # Load the data from the CSV file
    data = pd.read_csv(file_path)

    # check if there are consecutive numbers missing in the ConsumerCount column by comparing if they exist in other fileIDs.
    # If there are missing numbers, copy the row from the fileID where the consumerCount exists to the fileID where the consumerCount is missing
    # This is done to ensure that the ConsumerCount is the same for all fileIDs
    rows_to_append = []

    # Iterate over unique FileId values
    for fileID in data['FileId'].unique():
        file_data = data[data['FileId'] == fileID]

        # Iterate over unique topics for the current fileID
        for topic in file_data['Topic'].unique():
            topic_data = file_data[file_data['Topic'] == topic]

            # Iterate over consumer counts
            for consumerCount in range(1, topic_data['ConsumerCount'].max() + 1):

                # Check if consumer count is missing for the current topic
                if consumerCount not in topic_data['ConsumerCount'].values:

                    # Try to find the missing consumer count in other fileIDs
                    for fileID_copy in range(1, 5):
                        current = "log-"+str(fileID_copy)
                        if current != fileID:
                            file_data_copy = data[data['FileId'] == current]
                            file_data_copy = file_data_copy[(file_data_copy['ConsumerCount'] == consumerCount) & (file_data_copy['Topic'] == topic)]

                            # If found, modify the FileId and store the row to append later
                            if len(file_data_copy) > 0:
                                file_data_copy['FileId'] = fileID
                                rows_to_append.extend(file_data_copy.values.tolist())
                                # print(f'ConsumerCount {consumerCount} copied from fileID {fileID_copy} to fileID {fileID}')
                                break

    # Append the rows after the loop
    data = pd.concat([data, pd.DataFrame(rows_to_append, columns=data.columns)])


    # Separate the data into two dataframes based on the Topic
    data_priority = data[data['Topic'] == topic1]
    data_normal = data[data['Topic'] == topic2]

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

    return max_time_diff_priority, max_time_diff_normal

def analyze_time_differences(time_diffs_star, file_labels):
    """
    Analyze and print out metrics such as max, mean, 25th, 50th and 75th percentiles for the time differences.
    """
    print("Analysis Results:\n")
    for file_name, (time_diff_priority, time_diff_normal) in time_diffs_star.items():
        print(f"For configuration: {file_labels[file_name]}")

        # Priority
        print("  Priority events:")
        print(f"    Max: {time_diff_priority.max()}")
        print(f"    Mean: {time_diff_priority.mean()}")
        print(f"    25th percentile: {time_diff_priority.quantile(0.25)}")
        print(f"    50th percentile: {time_diff_priority.quantile(0.50)}")
        print(f"    75th percentile: {time_diff_priority.quantile(0.75)}\n")

        # Normal
        print("  Normal events:")
        print(f"    Max: {time_diff_normal.max()}")
        print(f"    Mean: {time_diff_normal.mean()}")
        print(f"    25th percentile: {time_diff_normal.quantile(0.25)}")
        print(f"    50th percentile: {time_diff_normal.quantile(0.50)}")
        print(f"    75th percentile: {time_diff_normal.quantile(0.75)}\n")

# List of file paths
file_paths = ['../star_topology/s_w1_p1_consumer_count.csv',
              '../star_topology/s_w2_p1_consumer_count.csv',
              '../star_topology/s_w3_p1_consumer_count.csv',
              ]

# Dictionary to store average Consumer Lags for each file
average_time_diffs = {}

# Process each file
for file_path in file_paths:
    average_time_diffs[file_path.split('/')[-1]] = process_file(file_path, 'topic_priority', 'topic_normal')

# Define labels for each file name
file_labels = {
    f's_w1_p1_consumer_count.csv': 'Undersaturated Workload',
    f's_w2_p1_consumer_count.csv': 'Fluctuating Workload',
    f's_w3_p1_consumer_count.csv': 'Oversaturated Workload',
}

# Preparing data for boxplot
all_data = []
for file_name, (priority_data, normal_data) in average_time_diffs.items():
    label = file_labels.get(file_name, file_name)
    all_data.append(pd.DataFrame({
        'Consumer Lag [seconds]': priority_data.values,
        'Events Consumed': priority_data.index,
        'Type': 'Priority',
        'Policy': label
    }))
    all_data.append(pd.DataFrame({
        'Consumer Lag [seconds]': normal_data.values,
        'Events Consumed': normal_data.index,
        'Type': 'Normal',
        'Policy': label
    }))

all_data_df = pd.concat(all_data)

# Extract Seaborn's default blue and red colors
sns_blue = sns.color_palette()[0]
sns_red = sns.color_palette()[3]

# Define the color palette
palette = {
    'Priority': sns_red,
    'Normal': sns_blue
}

# Boxplot rotated by 90 degrees with Seaborn's colors
plt.figure(figsize=(6, 4))
sns.boxplot(data=all_data_df, y='Policy', x='Consumer Lag [seconds]', hue='Type', palette=palette)


plt.legend(loc='best')
analyze_time_differences(average_time_diffs, file_labels)
plt.savefig(f's_w1-3_p1_lag_boxplot.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)



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

# List of file paths
file_paths = [f'sen_{i}_consumer_count.csv' for i in range(1, 17)]

# Dictionary to store average Consumer Lags for each file
time_diffs_star = {}

# Process each file
for file_path in file_paths:
    time_diffs_star[file_path.split('/')[-1]] = process_file(file_path, 'topic_priority', 'topic_normal')

# Define labels for each file name
# file_labels = {
#     f's_w2_p0_consumer_count.csv': 'Star Topology - Dyconits Disabled',
#     f's_w2_p1_consumer_count.csv': 'Star Topology - Dyconits Enabled',
#     f't_w2_p0_consumer_count.csv': 'Transitive Topology - Dyconits Disabled',
#     f't_w2_p1_consumer_count.csv': 'Transitive Topology - Dyconits Enabled',
# }
colors = sns.color_palette()

# Create a figure with 2 subplots
fig, axs = plt.subplots(2, 1, figsize=(12, 8))

# plot the data for both priority and normal in the separate subplot
color_cycle = cycle(colors)

avg_lag_prio = pd.DataFrame()
avg_lag_normal = pd.DataFrame()

# Plot for star topology
for i, (file_name, (all_time_diff_priority, all_time_diff_normal)) in enumerate(time_diffs_star.items()):
    axs[0].plot(all_time_diff_priority.index, all_time_diff_priority.values, label=file_name)
    axs[1].plot(all_time_diff_normal.index, all_time_diff_normal.values, label=file_name)

    # Calculate the average consumer lag for each df and save this to a new row in the avg_lag df
    avg_lag_prio.loc[file_name, 'avg_lag'] = all_time_diff_priority.mean()
    avg_lag_normal.loc[file_name, 'avg_lag'] = all_time_diff_normal.mean()


axs[1].set_xlabel('Events Consumed')

# # create a legend
# axs[0].legend(loc='upper left', bbox_to_anchor=(1.05, 1), frameon=False)
# axs[1].legend(loc='upper left', bbox_to_anchor=(1.05, 1), frameon=False)

for ax in axs:

    # put the legend on the right side of the plot
    ax.legend(loc='best', frameon=False)

    # set the y-axis ticks
    # ax.set_yticks([x for x in range(0, 7, 1)])
    ax.grid(True)
    ax.set_ylabel('Consumer Lag (s)')
    # ax.set_xticks(np.arange(50, 160, 10))

plt.savefig(f'../e5/sen_all_lag.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)

print(avg_lag_prio)
print(avg_lag_normal)

# save the dataframes to csv
avg_lag_prio.to_csv('sen_avg_lag_prio.csv')
avg_lag_normal.to_csv('sen_avg_lag_normal.csv')



import pandas as pd
import matplotlib.pyplot as plt

# Load the data from the CSV file
data = pd.read_csv('consumer_count.csv')

# Separate the data into two dataframes based on the Topic
data_priority = data[data['Topic'] == 'topic_priority']
data_normal = data[data['Topic'] == 'topic_normal']

# Convert the Time column from string to datetime
data_priority['Time'] = pd.to_datetime(data_priority['Time'])
data_normal['Time'] = pd.to_datetime(data_normal['Time'])

# Sort the data by ConsumerCount and Time
data_priority = data_priority.sort_values(['ConsumerCount', 'Time'])
data_normal = data_normal.sort_values(['ConsumerCount', 'Time'])

# Calculate the time difference between consecutive rows
data_priority['TimeDifference'] = data_priority.groupby('ConsumerCount')['Time'].diff()
data_normal['TimeDifference'] = data_normal.groupby('ConsumerCount')['Time'].diff()

# Convert the time difference to seconds for easier analysis
data_priority['TimeDifference'] = data_priority['TimeDifference'].dt.total_seconds()
data_normal['TimeDifference'] = data_normal['TimeDifference'].dt.total_seconds()

# Calculate the average time difference for each ConsumerCount for each Topic
average_time_diff_priority = data_priority.groupby('ConsumerCount')['TimeDifference'].mean()
average_time_diff_normal = data_normal.groupby('ConsumerCount')['TimeDifference'].mean()

# Plot the average time difference for each ConsumerCount for topic_priority
plt.figure(figsize=(10, 6))
plt.plot(average_time_diff_priority.index, average_time_diff_priority.values, marker='o', label='topic_priority')
plt.title('Average Time Difference for each ConsumerCount for topic_priority')
plt.xlabel('ConsumerCount')
plt.ylabel('Average Time Difference (seconds)')
plt.grid(True)
plt.legend()
plt.show()

# Plot the average time difference for each ConsumerCount for topic_normal
plt.figure(figsize=(10, 6))
plt.plot(average_time_diff_normal.index, average_time_diff_normal.values, marker='o', color='green', label='topic_normal')
plt.title('Average Time Difference for each ConsumerCount for topic_normal')
plt.xlabel('ConsumerCount')
plt.ylabel('Average Time Difference (seconds)')
plt.grid(True)
plt.legend()
plt.show()

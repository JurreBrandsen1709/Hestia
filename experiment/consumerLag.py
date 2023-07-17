import pandas as pd
import re
import matplotlib.pyplot as plt

log_files = ['C:\\Users\\JurreB\\Documents\\kafka-dotnet-getting-started\\star_topology\\C1\\log_20230717_110726.txt',
             'C:\\Users\\JurreB\\Documents\\kafka-dotnet-getting-started\\star_topology\\C2\\log_20230717_110726.txt',
             'C:\\Users\\JurreB\\Documents\\kafka-dotnet-getting-started\\star_topology\\C3\\log_20230717_110728.txt',
             'C:\\Users\\JurreB\\Documents\\kafka-dotnet-getting-started\\star_topology\\C4\\log_20230717_110728.txt']
lines_with_topic_count = []

for log_file in log_files:
    with open(log_file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            m = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ \+\d{2}:\d{2} \[INF\] Topic: (\S+) - consumer count (\d+)', line)
            if m:
                lines_with_topic_count.append(line.strip())

df = pd.DataFrame(lines_with_topic_count, columns=['message'])

# Filter the DataFrame to only include rows with consumer count information
consumer_df = df[df['message'].str.contains('consumer count')]

# Extract the consumer count from the message column
consumer_df['consumer_count'] = consumer_df['message'].str.extract(r'consumer count (\d+)')

# Extract the topic from the message column
consumer_df['topic'] = consumer_df['message'].str.extract(r'Topic: (\S+)')

# Extract the datetime from the message column
consumer_df['timestamp'] = consumer_df['message'].str.extract(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)')

# Convert the timestamp column to a datetime object
consumer_df['timestamp'] = pd.to_datetime(consumer_df['timestamp'])

# Drop the message column
consumer_df = consumer_df.drop(columns=['message'])

# group the dataframe by the consumer count value and topic
grouped_df = consumer_df.groupby(['consumer_count', 'topic'])

# create an empty dictionary to store the results
results = {}

# loop over each group
for name, group in grouped_df:
    # sort the group by timestamp
    sorted_group = group.sort_values(by='timestamp')

    # calculate the time differences between timestamps
    time_diffs = sorted_group['timestamp'].diff()

    # calculate the mean time difference and add it to the results dictionary
    mean_time_diff = time_diffs.mean().total_seconds()
    results[name] = mean_time_diff

# create a new DataFrame from the results dictionary
results_df = pd.DataFrame.from_dict(results, orient='index', columns=['mean_time_diff'])

# reset the index so that consumer_count and topic are columns
results_df = results_df.reset_index()

# Split the index column into separate consumer_count and topic columns
results_df[['consumer_count', 'topic']] = pd.DataFrame(results_df['index'].tolist(), index=results_df.index)
results_df = results_df.drop(columns=['index'])

# turn consumer_count into an integer
results_df['consumer_count'] = results_df['consumer_count'].astype(int)

# Group the results by topic and by numerical order for consumer count going from 1 to 10.
results_df = results_df.sort_values('consumer_count')
grouped_df = results_df.groupby('topic')




# Plot the results for each topic separately
for topic, group in grouped_df:
    fig, ax = plt.subplots()
    ax.plot(group['consumer_count'], group['mean_time_diff'])
    ax.set_xlabel('Consumer Count')
    ax.set_ylabel('Average Time Difference (s)')
    ax.set_title(topic)
    plt.show()



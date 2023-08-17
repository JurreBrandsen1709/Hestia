import pandas as pd

def calculate_avg_throughput(filename):
    # Load the data from the file into a pandas DataFrame
    data = pd.read_csv(filename)

    # Filter out rows with zero throughputs
    filtered_data = data[data['Throughput'] != 0]

    # Calculate the average throughput for both topics
    average_throughput = filtered_data.groupby('Topic')['Throughput'].mean()

    return average_throughput

# List of filenames
filenames = [f'sen_{i}_overhead_throughput.csv' for i in range(1, 17)]

# Lists to store temporary dataframes for each topic
topic_priority_results = []
topic_normal_results = []

# Loop through each filename and calculate average throughput
for filename in filenames:
    avg_throughput = calculate_avg_throughput(filename)

    # Create temporary dataframes and add to respective lists
    topic_priority_results.append(pd.DataFrame({
        'File': [filename],
        'Average Throughput': [avg_throughput.get('topic_priority', None)]
    }))

    topic_normal_results.append(pd.DataFrame({
        'File': [filename],
        'Average Throughput': [avg_throughput.get('topic_normal', None)]
    }))

# Concatenate results into final dataframes
topic_priority_df = pd.concat(topic_priority_results, ignore_index=True)
topic_normal_df = pd.concat(topic_normal_results, ignore_index=True)


print("Average Throughput for topic_priority:")
print(topic_priority_df)

print("\nAverage Throughput for topic_normal:")
print(topic_normal_df)

# save the dataframes to csv files
topic_priority_df.to_csv('sen_avg_overhead_throughput_topic_priority.csv', index=False)
topic_normal_df.to_csv('sen_avg_overhead_throughput_topic_normal.csv', index=False)


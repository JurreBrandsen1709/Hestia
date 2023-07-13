import datetime
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def extract_information(file):
    overhead_throughput = {}
    message_throughput = {}
    consumer_count = {}
    cpu_utilization = {}

    with open(file, "r") as f:
        for line in f:
            # Extract time
            time = re.search(r'(\d{2}:\d{2}:\d{2}\.\d{3})', line).group(1)

            # Extract overhead throughput
            overhead_match = re.search(r"Collection name: (\w+) overhead throughput (\d+) messages/s", line)
            if overhead_match:
                collection_name = overhead_match.group(1)
                throughput = int(overhead_match.group(2))

                if collection_name not in overhead_throughput:
                    overhead_throughput[collection_name] = []
                overhead_throughput[collection_name].append((time, throughput))

            # Extract message throughput
            topic_match = re.search(r'Topic\s(\w+)', line)
            throughput_match = re.search(r'throughput:\s([\d,]+)', line)
            if topic_match and throughput_match:
                topic = topic_match.group(1)
                throughput = float(throughput_match.group(1).replace(',', '.'))

                if topic not in message_throughput:
                    message_throughput[topic] = []
                message_throughput[topic].append((time, throughput))

            # Extract consumer count
            consumer_match = re.search(r"Topic: (\w+) - consumer count (\d+)", line)
            if consumer_match:
                topic = consumer_match.group(1)
                count = int(consumer_match.group(2))

                if topic not in consumer_count:
                    consumer_count[topic] = []
                consumer_count[topic].append((time, count))

            # Extract CPU utilization
            port_match = re.search(r'port:\s(\d+)', line)
            if port_match:
                port = port_match.group(1)
                utilization = re.search(r'Utilization:\s([\d,]+)%', line)
                if utilization:
                    utilization = float(utilization.group(1).replace(',', '.'))

                    if port not in cpu_utilization:
                        cpu_utilization[port] = []
                    cpu_utilization[port].append((time, utilization))

    return overhead_throughput, message_throughput, consumer_count, cpu_utilization


file_path = 'C:\\Users\\JurreB\Documents\\kafka-dotnet-getting-started\\a-b_config\\Consumer_a\\log_20230713_120637.txt'

overhead_throughput, message_throughput, consumer_count, cpu_utilization = extract_information(file_path)

# Convert data to pandas DataFrame
overhead_df = pd.DataFrame([(collection, time, throughput) for collection, data in overhead_throughput.items()
                            for time, throughput in data], columns=['Collection', 'Time', 'Throughput'])

message_df = pd.DataFrame([(topic, time, throughput) for topic, data in message_throughput.items()
                           for time, throughput in data], columns=['Topic', 'Time', 'Throughput'])

consumer_df = pd.DataFrame([(topic, time, count) for topic, data in consumer_count.items()
                            for time, count in data], columns=['Topic', 'Time', 'ConsumerCount'])

cpu_df = pd.DataFrame([(port, time, utilization) for port, data in cpu_utilization.items()
                       for time, utilization in data], columns=['Port', 'Time', 'Utilization'])

# Save data to CSV
overhead_df.to_csv('overhead_throughput.csv', index=False)
message_df.to_csv('message_throughput.csv', index=False)
consumer_df.to_csv('consumer_count.csv', index=False)
cpu_df.to_csv('cpu_utilization.csv', index=False)


# Load the CSV file into a pandas DataFrame
df = pd.read_csv('consumer_count.csv')

# Convert the Time column to a datetime format
df['Time'] = pd.to_datetime(df['Time'], format='%H:%M:%S.%f')

# Calculate the elapsed time in seconds from the start of the recording
df['Elapsed'] = (df['Time'] - df['Time'][0]).dt.total_seconds()

# Rename the columns
df.rename(columns={'Elapsed': 'Seconds', 'ConsumerCount': 'Count'}, inplace=True)

# Create separate DataFrames for each topic
df_topic1 = df[df['Topic'] == 'topic_normal']
df_topic2 = df[df['Topic'] == 'topic_priority']

# Plot the DataFrames with different colors
plt.plot(df_topic1['Seconds'], df_topic1['Count'], color='blue', label='Topic Normal')
plt.plot(df_topic2['Seconds'], df_topic2['Count'], color='red', label='Topic Priority')
plt.xticks(range(0, int(df['Seconds'].max()) + 1, 10))
plt.xticks(rotation=45)
plt.xlabel('Seconds')
plt.ylabel('Consumer Count')
plt.legend()
plt.show()
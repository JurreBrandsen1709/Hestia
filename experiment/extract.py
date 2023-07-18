import pandas as pd
import re

def extract_information(file, file_id):
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
                overhead_throughput[collection_name].append((time, throughput, file_id))

            # Extract message throughput
            topic_match = re.search(r'Topic\s(\w+)', line)
            throughput_match = re.search(r'throughput:\s([\d,]+)', line)
            if topic_match and throughput_match:
                topic = topic_match.group(1)
                throughput = float(throughput_match.group(1).replace(',', '.'))

                if topic not in message_throughput:
                    message_throughput[topic] = []
                message_throughput[topic].append((time, throughput, file_id))

            # Extract consumer count
            consumer_match = re.search(r"Topic: (\w+) - consumer count (\d+)", line)
            if consumer_match:
                topic = consumer_match.group(1)
                count = int(consumer_match.group(2))

                if topic not in consumer_count:
                    consumer_count[topic] = []
                consumer_count[topic].append((time, count, file_id))

            # Extract CPU utilization
            port_match = re.search(r'port:\s(\d+)', line)
            if port_match:
                port = port_match.group(1)
                utilization = re.search(r'Utilization:\s([\d,]+)%', line)
                if utilization:
                    utilization = float(utilization.group(1).replace(',', '.'))

                    if port not in cpu_utilization:
                        cpu_utilization[port] = []
                    cpu_utilization[port].append((time, utilization, file_id))

    return overhead_throughput, message_throughput, consumer_count, cpu_utilization

file_path = 'C:\\Users\\JurreB\\Documents\\kafka-dotnet-getting-started\\star_topology\\'
 # C:\Users\JurreB\Documents\kafka-dotnet-getting-started\star_topology\C1\log_20230718_115352.txt
 # C:\Users\JurreB\Documents\kafka-dotnet-getting-started\star_topology\C1\log_20230718_132236.txt
# C:\Users\JurreB\Documents\kafka-dotnet-getting-started\star_topology\C1\log_20230718_140628.txt

# List of file paths
file_paths = [file_path + 'C1\\log_20230718_140628.txt',
              file_path + 'C2\\log_20230718_140628.txt',
              file_path + 'C3\\log_20230718_140628.txt',
              file_path + 'C4\\log_20230718_140629.txt']


# DataFrames
overhead_df = pd.DataFrame(columns=['Collection', 'Time', 'Throughput', 'FileId'])
message_df = pd.DataFrame(columns=['Topic', 'Time', 'Throughput', 'FileId'])
consumer_df = pd.DataFrame(columns=['Topic', 'Time', 'ConsumerCount', 'FileId'])
cpu_df = pd.DataFrame(columns=['Port', 'Time', 'Utilization', 'FileId'])

# Loop through each file and append data to DataFrames
for file_id, file_path in enumerate(file_paths, start=1):
    overhead_throughput, message_throughput, consumer_count, cpu_utilization = extract_information(file_path, f'log-{file_id}')

    overhead_df = pd.concat([overhead_df, pd.DataFrame([(collection, time, throughput, file_id) for collection, data in overhead_throughput.items()
                                                   for time, throughput, file_id in data], columns=['Collection', 'Time', 'Throughput', 'FileId'])], ignore_index=True)

    message_df = pd.concat([message_df, pd.DataFrame([(topic, time, throughput, file_id) for topic, data in message_throughput.items()
                                                 for time, throughput, file_id in data], columns=['Topic', 'Time', 'Throughput', 'FileId'])], ignore_index=True)

    consumer_df = pd.concat([consumer_df, pd.DataFrame([(topic, time, count, file_id) for topic, data in consumer_count.items()
                                                   for time, count, file_id in data], columns=['Topic', 'Time', 'ConsumerCount', 'FileId'])], ignore_index=True)

    cpu_df = pd.concat([cpu_df, pd.DataFrame([(port, time, utilization, file_id) for port, data in cpu_utilization.items()
                                          for time, utilization, file_id in data], columns=['Port', 'Time', 'Utilization', 'FileId'])], ignore_index=True)

# Save data to CSV
overhead_df.to_csv('overhead_throughput.csv', index=False)
message_df.to_csv('message_throughput.csv', index=False)
consumer_df.to_csv('consumer_count.csv', index=False)
cpu_df.to_csv('cpu_utilization.csv', index=False)
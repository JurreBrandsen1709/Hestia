import matplotlib.pyplot as plt

# Overhead throughput
topic_priority_overhead = [('10:42:30.234', 0), ('10:42:40.242', 0), ('10:42:50.254', 0)]
topic_normal_overhead = [('10:42:30.245', 0), ('10:42:40.246', 0), ('10:42:50.256', 0)]

# Message throughput
topic_priority_message = [('10:42:49.444', '0.1995438427754154'), ('10:42:59.430', '2.1961439665007387')]
topic_normal_message = [('10:42:51.440', '0.19936999877865938'), ('10:43:01.439', '1.398654075183451')]

# Consumer count
topic_priority_consumer = [('10:42:44.442', 1), ('10:42:51.250', 2), ('10:42:51.750', 3)]
topic_normal_consumer = [('10:42:46.434', 1), ('10:42:53.272', 2), ('10:42:53.782', 3)]

# CPU utilization
cpu_utilization = [('10:42:44.434', '6.25'), ('10:42:46.424', '6.25'), ('10:42:50.741', '18.75')]

# Extract x and y values for each data
topic_priority_overhead_x = [entry[0] for entry in topic_priority_overhead]
topic_priority_overhead_y = [entry[1] for entry in topic_priority_overhead]

topic_normal_overhead_x = [entry[0] for entry in topic_normal_overhead]
topic_normal_overhead_y = [entry[1] for entry in topic_normal_overhead]

topic_priority_message_x = [entry[0] for entry in topic_priority_message]
topic_priority_message_y = [float(entry[1]) for entry in topic_priority_message]

topic_normal_message_x = [entry[0] for entry in topic_normal_message]
topic_normal_message_y = [float(entry[1]) for entry in topic_normal_message]

topic_priority_consumer_x = [entry[0] for entry in topic_priority_consumer]
topic_priority_consumer_y = [entry[1] for entry in topic_priority_consumer]

topic_normal_consumer_x = [entry[0] for entry in topic_normal_consumer]
topic_normal_consumer_y = [entry[1] for entry in topic_normal_consumer]

cpu_utilization_x = [entry[0] for entry in cpu_utilization]
cpu_utilization_y = [float(entry[1]) for entry in cpu_utilization]

# Plotting
plt.figure(figsize=(12, 8))

# Overhead throughput plot
plt.subplot(2, 2, 1)
plt.plot(topic_priority_overhead_x, topic_priority_overhead_y, label='topic_priority')
plt.plot(topic_normal_overhead_x, topic_normal_overhead_y, label='topic_normal')
plt.xlabel('Time')
plt.ylabel('Throughput')
plt.title('Overhead Throughput')
plt.legend()

# Message throughput plot
plt.subplot(2, 2, 2)
plt.plot(topic_priority_message_x, topic_priority_message_y, label='topic_priority')
plt.plot(topic_normal_message_x, topic_normal_message_y, label='topic_normal')
plt.xlabel('Time')
plt.ylabel('Throughput')
plt.title('Message Throughput')
plt.legend()

# Consumer count plot
plt.subplot(2, 2, 3)
plt.plot(topic_priority_consumer_x, topic_priority_consumer_y, label='topic_priority')
plt.plot(topic_normal_consumer_x, topic_normal_consumer_y, label='topic_normal')
plt.xlabel('Time')
plt.ylabel('Consumer Count')
plt.title('Consumer Count')
plt.legend()

# CPU utilization plot
plt.subplot(2, 2, 4)
plt.plot(cpu_utilization_x, cpu_utilization_y)
plt.xlabel('Time')
plt.ylabel('CPU Utilization')
plt.title('CPU Utilization')

# Adjust subplots spacing
plt.tight_layout()

# Display the plots
plt.show()

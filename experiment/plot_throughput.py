import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Load the CSV file into a DataFrame
df = pd.read_csv('e1_normal_message_throughput.csv')

# Convert the 'Time' column to datetime format
df['Time'] = pd.to_datetime(df['Time']).dt.time

# # Convert 'Time' back to datetime.time from string
# df['Time'] = pd.to_datetime(df['Time']).dt.time

# Create a 'Seconds' column that represents the number of seconds past the hour
df['Seconds'] = df['Time'].apply(lambda t: t.hour*3600 + t.minute*60 + t.second + t.microsecond/1e6)

# Adjust the 'Seconds' column so that it starts from 0 for each FileId
for file_id in df['FileId'].unique():
    min_seconds = df.loc[df['FileId'] == file_id, 'Seconds'].min()
    df.loc[df['FileId'] == file_id, 'Seconds'] -= min_seconds

# Round 'Seconds' to the nearest 15 seconds
df['Seconds'] = (df['Seconds'] / 30).round() * 30

# Create a new DataFrame for plotting
plot_df = df[['Seconds', 'Topic', 'Throughput']].copy()

# Set contrasting colors for the plot
colors = ['blue', 'orange']

# Plot the throughput over time for each topic, averaging the values over the nodes
plt.figure(figsize=(12, 6))

for color, collection in zip(colors, plot_df['Topic'].unique()):
    subset_df = plot_df[plot_df['Topic'] == collection]
    sns.lineplot(x='Seconds', y='Throughput', data=subset_df, label=collection, color=color)

plt.title('Overhead Throughput Over Time')
plt.xlabel('Time (seconds)')
plt.ylabel('Throughput (average messages per second))')
plt.legend(title='Topic')
plt.grid(True)

# Set x-axis labels as seconds past the hour
labels = plot_df['Seconds'].unique()
plt.xticks(ticks=range(0, int(labels.max()), 30), rotation=45)

plt.tight_layout()
plt.show()

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the CSV files into pandas DataFrames
normal_df = pd.read_csv('docker/p0_message_throughput.csv')
p1_df = pd.read_csv('docker/p1_message_throughput.csv')
p2_df = pd.read_csv('docker/p2_message_throughput.csv')
p3_df = pd.read_csv('docker/p3_message_throughput.csv')

# Convert the 'Time' column to datetime
normal_df['Time'] = pd.to_datetime(normal_df['Time'])
p1_df['Time'] = pd.to_datetime(p1_df['Time'])
p2_df['Time'] = pd.to_datetime(p2_df['Time'])
p3_df['Time'] = pd.to_datetime(p3_df['Time'])

# Remove throughput values of 0
normal_df = normal_df[normal_df['Throughput'] > 0]
p1_df = p1_df[p1_df['Throughput'] > 0]
p2_df = p2_df[p2_df['Throughput'] > 0]
p3_df = p3_df[p3_df['Throughput'] > 0]

# Define the color palette
colors = sns.color_palette("tab10", n_colors=4)  # 4 colors for 4 policies

# Map the colors to the policies
color_dict = {
    'No Sync - Priority': colors[0],
    'No Sync - Normal': colors[0],
    'Policy 1 - Priority': colors[1],
    'Policy 1 - Normal': colors[1],
    'Policy 2 - Priority': colors[2],
    'Policy 2 - Normal': colors[2],
    'Policy 3 - Priority': colors[3],
    'Policy 3 - Normal': colors[3]
}

# Separate dataframes by topic
normal_priority = normal_df[normal_df['Topic'] == 'topic_priority']
normal_normal = normal_df[normal_df['Topic'] == 'topic_normal']
p1_priority = p1_df[p1_df['Topic'] == 'topic_priority']
p1_normal = p1_df[p1_df['Topic'] == 'topic_normal']
p2_priority = p2_df[p2_df['Topic'] == 'topic_priority']
p2_normal = p2_df[p2_df['Topic'] == 'topic_normal']
p3_priority = p3_df[p3_df['Topic'] == 'topic_priority']
p3_normal = p3_df[p3_df['Topic'] == 'topic_normal']

# Add a new 'Configuration' column to distinguish them
normal_priority['Configuration'] = 'No Sync - Priority'
p1_priority['Configuration'] = 'Policy 1 - Priority'
p2_priority['Configuration'] = 'Policy 2 - Priority'
p3_priority['Configuration'] = 'Policy 3 - Priority'
normal_normal['Configuration'] = 'No Sync - Normal'
p1_normal['Configuration'] = 'Policy 1 - Normal'
p2_normal['Configuration'] = 'Policy 2 - Normal'
p3_normal['Configuration'] = 'Policy 3 - Normal'

# Combine all dataframes
all_data = pd.concat([normal_priority, p1_priority, p2_priority, p3_priority, normal_normal, p1_normal, p2_normal, p3_normal])

# Create the violin plot
plt.figure(figsize=(12, 6))
sns.violinplot(x="Configuration", y="Throughput", data=all_data, cut=0, palette=color_dict)
plt.xticks(rotation=45)
plt.title('Throughput Distributions for Different Configurations')
plt.ylim(bottom=0)
plt.savefig('docker/e1_w2_message_throughput.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)

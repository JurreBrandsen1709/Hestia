import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def process_df(path, config_priority, config_normal):
    df = pd.read_csv(path)
    df['Time'] = pd.to_datetime(df['Time'])
    df = df[df['Throughput'] > 0]

    df_priority = df[df['Topic'] == 'topic_priority']
    df_normal = df[df['Topic'] == 'topic_normal']

    df_priority['Configuration'] = config_priority
    df_normal['Configuration'] = config_normal

    return pd.concat([df_priority, df_normal])

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

# Define the file paths and corresponding configurations
paths_configs = [
    ('docker/p0_overhead_throughput.csv', 'No Sync - Priority', 'No Sync - Normal'),
    ('docker/p1_overhead_throughput.csv', 'Policy 1 - Priority', 'Policy 1 - Normal'),
    ('docker/p2_overhead_throughput.csv', 'Policy 2 - Priority', 'Policy 2 - Normal'),
    ('docker/p3_overhead_throughput.csv', 'Policy 3 - Priority', 'Policy 3 - Normal'),
]

# Process each CSV file and combine all dataframes
all_data = pd.concat([process_df(path, config_priority, config_normal) for path, config_priority, config_normal in paths_configs])

# Create the violin plot
plt.figure(figsize=(10, 6))
sns.violinplot(x="Configuration", y="Throughput", data=all_data, cut=0, palette=color_dict)
plt.xticks(rotation=45)
plt.title('Throughput Distributions for Different Configurations')
plt.ylim(bottom=0)
plt.savefig('docker/e1_w2_overhead_throughput.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)

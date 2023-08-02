import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def load_and_process_data(file_path, configuration):
    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)

    # Convert the 'Time' column to datetime
    df['Time'] = pd.to_datetime(df['Time'])

    # Remove throughput values of 0
    df = df[df['Throughput'] > 0]

    # Separate dataframe by topic
    priority_df = df[df['Topic'] == 'topic_priority']
    normal_df = df[df['Topic'] == 'topic_normal']

    # Add a new 'Configuration' column to distinguish them
    priority_df['Configuration'] = f'{configuration} - Priority'
    normal_df['Configuration'] = f'{configuration} - Normal'

    return priority_df, normal_df

# Load and process the data
normal_priority, normal_normal = load_and_process_data('improvement/s_w1_p1_message_throughput.csv', 'No Sync')
p1_priority, p1_normal = load_and_process_data('improvement/s_w1_p2_message_throughput.csv', 'Policy 1')

# Combine all dataframes
all_data = pd.concat([normal_priority, normal_normal, p1_priority, p1_normal])

# Define the color palette
colors = sns.color_palette("tab10", n_colors=4)  # 4 colors for 4 policies

# Map the colors to the configurations
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

# Create the violin plot
plt.figure(figsize=(12, 6))
sns.violinplot(x="Configuration", y="Throughput", data=all_data, cut=0, palette=color_dict)
plt.xticks(rotation=45)
plt.title('Throughput Distributions for Different Configurations')
plt.ylim(bottom=0)
plt.savefig('improvement/s_w1_throughput.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)

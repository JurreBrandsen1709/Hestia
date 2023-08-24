import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def load_and_process_data(file_path, configuration, topic1, topic2):
    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)

    # Convert the 'Time' column to datetime
    df['Time'] = pd.to_datetime(df['Time'])

    # Remove throughput values of 0
    df = df[df['Throughput'] > 0]

    # Separate dataframe by topic
    priority_df = df[df['Topic'] == topic1]
    normal_df = df[df['Topic'] == topic2]

    if topic1 == 'trans_topic_priority':
        # also add the topic_priority to the priority_df
        priority_df['Config_Topic'] = f'{configuration} Multi-hop Priority'
    else:
        # also add the topic_normal to the priority_df
        priority_df['Config_Topic'] = f'{configuration} Priority'

    if topic2 == 'trans_topic_normal':
        # also add the topic_normal to the normal_df
        normal_df['Config_Topic'] = f'{configuration} Multi-hop Normal'
    else:
        # also add the topic_normal to the normal_df
        normal_df['Config_Topic'] = f'{configuration} Normal'

    return priority_df, normal_df

def plot_with_legend(all_data, color_dict, hatch_dict):
    plt.figure(figsize=(10, 4))

    ax = sns.boxplot(x="Throughput", y="Config_Topic", data=all_data, order=color_dict.keys(), palette=color_dict)

    # Modify y-axis labels for simplicity
    new_labels = ['Multi-hop Priority' if 'Multi-hop Priority' in label.get_text() else 'Priority' if 'Priority' in label.get_text() else 'Multi-hop Normal' if 'Multi-hop Normal' in label.get_text() else 'Normal' for label in ax.get_yticklabels()]
    ax.set_yticklabels(new_labels)

    # Create custom legend
    legend_elements = [
        plt.Line2D([0], [0], color=sns.color_palette('muted')[0], lw=4, label='Star - Disabled'),
        plt.Line2D([0], [0], color=sns.color_palette()[0], lw=4, label='Star - Enabled'),
        plt.Line2D([0], [0], color=sns.color_palette('muted')[3], lw=4, label='Multi-hop - Disabled'),
        plt.Line2D([0], [0], color=sns.color_palette()[3], lw=4, label='Multi-hop - Enabled'),
    ]

    # place the legend outside the figure/axis
    plt.legend(handles=legend_elements, bbox_to_anchor=(1.05, 1), loc='upper left')

    plt.xlim(left=0)
    plt.xlabel('Throughput [messages/s]')
    plt.ylabel('Topic Type')
    plt.tight_layout()
    plt.savefig(f's-t_w2_p0-1_throughput_all.png', bbox_inches='tight', pad_inches=0.05, dpi=300)


all_data = pd.DataFrame()

# Load and process the data
configurations = ['Star topology - Dyconits Disabled', 'Star topology - Dyconits Enabled']
configurations2 = ['Multi-hop topology - Dyconits Disabled', 'Multi-hop topology - Dyconits Enabled']
file_paths = [f'../star_topology/s_w2_p{i}_message_throughput.csv' for i in range(0, 2)]
file_paths2 = [f'../trans_topology/t_w2_p{i}_message_throughput.csv' for i in range(0, 2)]
file_paths3 = [f'../trans_topology/t_w2_p{i}_message_throughput.csv' for i in range(0, 2)]

# ... [rest of the code remains unchanged]

for config, path in zip(configurations, file_paths):
    priority_df, normal_df = load_and_process_data(path, config, 'topic_priority', 'topic_normal')
    all_data = pd.concat([all_data, priority_df, normal_df])

    # Compute and print statistics for this configuration
    for df, topic_type in [(priority_df, "Priority"), (normal_df, "Normal")]:
        print(f"Configuration: {config} - {topic_type}")
        print(f"Max Throughput: {df['Throughput'].max()}")
        print(f"Mean Throughput: {df['Throughput'].mean()}")
        print(f"25th Percentile: {df['Throughput'].quantile(0.25)}")
        print(f"50th Percentile: {df['Throughput'].quantile(0.5)}")
        print(f"75th Percentile: {df['Throughput'].quantile(0.75)}")
        print("------------------------------------------------")

for config, path in zip(configurations2, file_paths2):
    priority_df, normal_df = load_and_process_data(path, config, 'topic_priority', 'topic_normal')
    all_data = pd.concat([all_data, priority_df, normal_df])

    # Compute and print statistics for this configuration
    for df, topic_type in [(priority_df, "Priority"), (normal_df, "Normal")]:
        print(f"Configuration: {config} - {topic_type}")
        print(f"Max Throughput: {df['Throughput'].max()}")
        print(f"Mean Throughput: {df['Throughput'].mean()}")
        print(f"25th Percentile: {df['Throughput'].quantile(0.25)}")
        print(f"50th Percentile: {df['Throughput'].quantile(0.5)}")
        print(f"75th Percentile: {df['Throughput'].quantile(0.75)}")
        print("------------------------------------------------")

for config, path in zip(configurations2, file_paths3):
    priority_df, normal_df = load_and_process_data(path, config, 'trans_topic_priority', 'trans_topic_normal')
    all_data = pd.concat([all_data, priority_df, normal_df])

    # Compute and print statistics for this configuration
    for df, topic_type in [(priority_df, "Multi-hop Priority"), (normal_df, "Multi-hop Normal")]:
        print(f"Configuration: {config} - {topic_type}")
        print(f"Max Throughput: {df['Throughput'].max()}")
        print(f"Mean Throughput: {df['Throughput'].mean()}")
        print(f"25th Percentile: {df['Throughput'].quantile(0.25)}")
        print(f"50th Percentile: {df['Throughput'].quantile(0.5)}")
        print(f"75th Percentile: {df['Throughput'].quantile(0.75)}")
        print("------------------------------------------------")

# ... [rest of the code remains unchanged]

# Color dictionary for different configurations
color_dict = {
    'Star topology - Dyconits Disabled Priority': sns.color_palette('muted')[0],
    'Star topology - Dyconits Enabled Priority': sns.color_palette()[0],
    'Multi-hop topology - Dyconits Disabled Priority': sns.color_palette('muted')[3],
    'Multi-hop topology - Dyconits Enabled Priority': sns.color_palette()[3],
    'Multi-hop topology - Dyconits Disabled Multi-hop Priority': sns.color_palette('muted')[3],
    'Multi-hop topology - Dyconits Enabled Multi-hop Priority': sns.color_palette()[3],
    'Star topology - Dyconits Disabled Normal': sns.color_palette('muted')[0],
    'Star topology - Dyconits Enabled Normal': sns.color_palette()[0],
    'Multi-hop topology - Dyconits Disabled Normal': sns.color_palette('muted')[3],
    'Multi-hop topology - Dyconits Enabled Normal': sns.color_palette()[3],
    'Multi-hop topology - Dyconits Disabled Multi-hop Normal': sns.color_palette('muted')[3],
    'Multi-hop topology - Dyconits Enabled Multi-hop Normal': sns.color_palette()[3],
}

# Hatch dictionary for Priority/Normal distinction
hatch_dict = {
    'Priority': '*',
    'Normal': '.'
}

sns.set_context("talk")
sns.set_style("whitegrid")
# print all configurations
plot_with_legend(all_data, color_dict, hatch_dict)
# Plot for all data
# plt.figure(figsize=(6, 4))
# ax = sns.boxplot(x="Throughput", y="Config_Topic", data=all_data, order=color_dict.keys(), palette=color_dict)

# # Apply hatching based on Priority/Normal distinction
# for i, box in enumerate(ax.artists):
#     topic_type = 'Priority' if 'Priority' in color_dict.keys()[i] else 'Normal'
#     box.set_hatch(hatch_dict[topic_type])

# plt.xlim(left=0)
# plt.xlabel('Throughput (messages/s)')
# plt.ylabel('')

# plt.tight_layout()
# plt.savefig(f'e2/s-t_w2_p0-1_throughput_all.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)

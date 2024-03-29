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

    # Add a new 'Config_Topic' column to distinguish them
    priority_df['Config_Topic'] = f'{configuration} Priority'
    normal_df['Config_Topic'] = f'{configuration} Normal'

    return priority_df, normal_df

workloads = ['w3']

for workload in workloads:
    all_data = pd.DataFrame()

    # Load and process the data
    configurations = ['None','S', 'MA', 'ES']
    file_paths = [f'../star_topology/s_{workload}_p{i}_overhead_throughput.csv' for i in range(0, 4)]

    for config, path in zip(configurations, file_paths):
        priority_df, normal_df = load_and_process_data(path, config)
        all_data = pd.concat([all_data, priority_df, normal_df])

    # Create color dictionary
    color_dict = {'None Priority': sns.color_palette()[3],  # orange
                  'S Priority': sns.color_palette()[3],  # orange
                  'MA Priority': sns.color_palette()[3],  # blue
                  'ES Priority': sns.color_palette()[3],  # green
                  'None Normal': sns.color_palette()[0],  # red
                  'S Normal': sns.color_palette()[0],  # red
                  'MA Normal': sns.color_palette()[0],
                  'ES Normal': sns.color_palette()[0],  # green
                  }  # green

    print("Unique Config_Topic values:", all_data['Config_Topic'].unique())

    grouped_data = all_data.groupby("Config_Topic")["Throughput"]
    max_values = grouped_data.max()
    mean_values = grouped_data.mean()
    percentiles = grouped_data.describe(percentiles=[.25, .5, .75])

    print("Throughput Analysis:")
    for config in color_dict.keys():
        print(f"\nConfiguration: {config}")
        print(f"Max: {max_values[config]:.2f}")
        print(f"Mean: {mean_values[config]:.2f}")
        print(f"25th percentile: {percentiles['25%'][config]:.2f}")
        print(f"50th percentile (Median): {percentiles['50%'][config]:.2f}")
        print(f"75th percentile: {percentiles['75%'][config]:.2f}")
        
    

    # Plot for all data
    sns.set_context("talk")
    sns.set_style("whitegrid")
    plt.figure(figsize=(6, 4))
    sns.boxplot(x="Throughput", y="Config_Topic", data=all_data, order=color_dict.keys(), palette=color_dict)
    plt.xlim(left=0)
    plt.xlabel('Overhead [Dyconit messages/s]')
    plt.ylabel('')

    plt.tight_layout()
    plt.savefig(f's_w3_p0-3_overhead_throughput_all.png', bbox_inches='tight', pad_inches=0.05, dpi=300)

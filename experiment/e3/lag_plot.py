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
    priority_data = pd.DataFrame()
    normal_data = pd.DataFrame()

    # Load and process the data
    configurations = ['Undersaturated Workload', 'Fluctuating Workload', 'Oversaturated Workload']
    file_paths = [f'../star_topology/s_w{i}_p1_consumer_count.csv' for i in range(1, 4)]

    for config, path in zip(configurations, file_paths):
        priority_df, normal_df = load_and_process_data(path, config)

        priority_data = pd.concat([priority_data, priority_df])
        normal_data = pd.concat([normal_data, normal_df])

    all_data = pd.concat([priority_data, normal_data])
    # Create color dictionary
    color_dict = {'Undersaturated Workload Priority': sns.color_palette()[3],  # orange
                  'Fluctuating Workload Priority': sns.color_palette()[3],  # orange
                  'Oversaturated Workload Priority': sns.color_palette()[3],  # blue
                  'Undersaturated Workload Normal': sns.color_palette()[0],  # red
                  'Fluctuating Workload Normal': sns.color_palette()[0],  # red
                  'Oversaturated Workload Normal': sns.color_palette()[0],
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
    sns.set_context("notebook")
    sns.set_style("whitegrid")
    plt.figure(figsize=(6, 4))
    sns.boxplot(x="Throughput", y="Config_Topic", data=all_data, order=color_dict.keys(), palette=color_dict)
    plt.xlim(left=0)
    plt.xlabel('Throughput [Dyconit Messages/s]')
    plt.ylabel('')

    plt.tight_layout()
    plt.savefig(f's_w1-3_p1_lag_plot.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

workloads = ['w3']

def bin_times(df, bin_size=0.1):
    """Bin times and average throughput for each bin."""
    bins = pd.interval_range(start=0, end=df['Time'].max() + bin_size, freq=bin_size)
    df['TimeBin'] = pd.cut(df['Time'], bins, labels=bins[:-1].mid)
    df_avg = df.groupby('TimeBin').Throughput.mean().reset_index()
    df_avg['Time'] = df_avg['TimeBin'].mid
    return df_avg.drop(columns='TimeBin')

def calculate_stats(df):
    """Calculate max, mean, and percentiles of Throughput."""
    max_throughput = df['Throughput'].max()
    mean_throughput = df['Throughput'].mean()
    percentiles = np.percentile(df['Throughput'], [25, 50, 75])  # Change percentiles as needed
    return max_throughput, mean_throughput, percentiles

def plot_and_save(df0, df1, filename):
    """Plot and save the throughput comparison."""
    # ... (your existing plotting code)

    max0, mean0, percentiles0 = calculate_stats(df0)
    max1, mean1, percentiles1 = calculate_stats(df1)

    print("Configuration 0:")
    print(f"Max Throughput: {max0}")
    print(f"Mean Throughput: {mean0}")
    print("Percentiles:")
    for i, percentile in enumerate([25, 50, 75]):
        print(f"{percentile}th Percentile: {percentiles0[i]}")

    print("\nConfiguration 1:")
    print(f"Max Throughput: {max1}")
    print(f"Mean Throughput: {mean1}")
    print("Percentiles:")
    for i, percentile in enumerate([25, 50, 75]):
        print(f"{percentile}th Percentile: {percentiles1[i]}")

for workload in workloads:

    # Load the CSV files
    df0_m = pd.read_csv(f'../star_topology/s_w2_p0_message_throughput.csv')
    df1_m = pd.read_csv(f'../star_topology/s_w2_p1_message_throughput.csv')

    # Convert the 'Time' column to datetime format and seconds
    df0_m['Time'] = (pd.to_datetime(df0_m['Time']) - pd.to_datetime(df0_m['Time']).min()).dt.total_seconds()
    df1_m['Time'] = (pd.to_datetime(df1_m['Time']) - pd.to_datetime(df1_m['Time']).min()).dt.total_seconds()

    # Remove all throughputs of 0
    df0_m = df0_m[df0_m['Throughput'] != 0]
    df1_m = df1_m[df1_m['Throughput'] != 0]

    # Separate the dataframes based on the 'Topic' column
    df0_normal_m = df0_m[df0_m['Topic'] == 'topic_normal']
    df0_priority_m = df0_m[df0_m['Topic'] == 'topic_priority']

    df1_normal_m = df1_m[df1_m['Topic'] == 'topic_normal']
    df1_priority_m = df1_m[df1_m['Topic'] == 'topic_priority']

    # # Normalize all dataframes to the same elapsed time
    max_time_normal_m = min(df0_normal_m['Time'].max(), df1_normal_m['Time'].max(), )
    max_time_priority_m = min(df0_priority_m['Time'].max(), df1_priority_m['Time'].max(), )

    df0_normal_m = df0_normal_m[df0_normal_m['Time'] <= max_time_normal_m]
    df1_normal_m = df1_normal_m[df1_normal_m['Time'] <= max_time_normal_m]

    df0_priority_m = df0_priority_m[df0_priority_m['Time'] <= max_time_priority_m]
    df1_priority_m = df1_priority_m[df1_priority_m['Time'] <= max_time_priority_m]

    # Calculate the average throughput over the different FileId's
    tolerance = 5
    df0_normal_m['RoundedTime'] = df0_normal_m['Time'].round(decimals=int(-np.log10(tolerance)))
    df0_normal_avg_m = df0_normal_m.groupby('RoundedTime').Throughput.mean().reset_index()

    df0_priority_m['RoundedTime'] = df0_priority_m['Time'].round(decimals=int(-np.log10(tolerance)))
    df0_priority_avg_m = df0_priority_m.groupby('RoundedTime').Throughput.mean().reset_index()

    df1_normal_m['RoundedTime'] = df1_normal_m['Time'].round(decimals=int(-np.log10(tolerance)))
    df1_normal_avg_m = df1_normal_m.groupby('RoundedTime').Throughput.mean().reset_index()

    df1_priority_m['RoundedTime'] = df1_priority_m['Time'].round(decimals=int(-np.log10(tolerance)))
    df1_priority_avg_m = df1_priority_m.groupby('RoundedTime').Throughput.mean().reset_index()


    df0_normal_avg_m = df0_normal_avg_m[df0_normal_avg_m['RoundedTime'] <= 201]
    df1_normal_avg_m = df1_normal_avg_m[df1_normal_avg_m['RoundedTime'] <= 201]

    df0_priority_avg_m = df0_priority_avg_m[df0_priority_avg_m['RoundedTime'] <= 201]
    df1_priority_avg_m = df1_priority_avg_m[df1_priority_avg_m['RoundedTime'] <= 201]

    plot_and_save(df0_priority_avg_m, df1_priority_avg_m, 'priority_throughput_comparison.pdf')
    plot_and_save(df0_normal_avg_m, df1_normal_avg_m, 'normal_throughput_comparison.pdf')

    plt.figure(figsize=(14, 10))  # Increase the figure size

    plt.subplot(2, 1, 1)

    # Plot with the specified colors and styles
    plt.plot(df0_priority_avg_m['RoundedTime'], df0_priority_avg_m['Throughput'], color='orange', linestyle='--', )
    plt.plot(df1_priority_avg_m['RoundedTime'], df1_priority_avg_m['Throughput'], color='red', linestyle='-', )
    plt.plot(df0_normal_avg_m['RoundedTime'], df0_normal_avg_m['Throughput'], color='cyan', linestyle='--', )
    plt.plot(df1_normal_avg_m['RoundedTime'], df1_normal_avg_m['Throughput'], color='blue', linestyle='-',)

    plt.xlabel('Time [seconds]', fontsize=14)  # Increase font size
    plt.ylabel('Average Message Throughput / second', fontsize=14)  # Increase font size
    plt.xticks(fontsize=12)  # Increase font size for x-axis ticks
    plt.yticks(fontsize=12)  # Increase font size for y-axis ticks
    plt.grid()

    plt.ylim(ymin=0)  # Set y-axis to start at 0

    plt.axvspan(60, 90, color='yellow', alpha=0.2)  # Adds shaded region between x=60 and x=80
    plt.axvspan(140, 170, color='yellow', alpha=0.2)  # Adds shaded region between x=130 and x=160


    # Custom legend entries
    from matplotlib.lines import Line2D
    legend_elements = [Line2D([0], [0], color='orange', linestyle='--', label='Priority Events (Dyconits Disabled)'),
                       Line2D([0], [0], color='red', linestyle='-', label='Priority Events (Dyconits Enabled)'),
                       Line2D([0], [0], color='cyan', linestyle='--', label='Normal Events (Dyconits Disabled)'),
                       Line2D([0], [0], color='blue', linestyle='-', label='Normal Events (Dyconits Enabled)')]

    plt.legend(handles=legend_elements, loc='best', fontsize=12)  # Increase font size for legend

    plt.tight_layout()
    plt.savefig(f's_w2_p0-1_message_throughput.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

workloads = ['w3']

for workload in workloads:

    # Load the CSV files
    df0_m = pd.read_csv(f'../star_topology/s_w3_p1_message_throughput.csv')
    df1_m = pd.read_csv(f'../star_topology/s_w3_p2_message_throughput.csv')
    df2_m = pd.read_csv(f'../star_topology/s_w3_p3_message_throughput.csv')
    df3_m = pd.read_csv(f'../star_topology/s_w3_p0_message_throughput.csv')

    # Convert the 'Time' column to datetime format and seconds
    df0_m['Time'] = (pd.to_datetime(df0_m['Time']) - pd.to_datetime(df0_m['Time']).min()).dt.total_seconds()
    df1_m['Time'] = (pd.to_datetime(df1_m['Time']) - pd.to_datetime(df1_m['Time']).min()).dt.total_seconds()
    df2_m['Time'] = (pd.to_datetime(df2_m['Time']) - pd.to_datetime(df2_m['Time']).min()).dt.total_seconds()
    df3_m['Time'] = (pd.to_datetime(df3_m['Time']) - pd.to_datetime(df3_m['Time']).min()).dt.total_seconds()

    # Remove all throughputs of 0
    df0_m = df0_m[df0_m['Throughput'] != 0]
    df1_m = df1_m[df1_m['Throughput'] != 0]
    df2_m = df2_m[df2_m['Throughput'] != 0]
    df3_m = df3_m[df3_m['Throughput'] != 0]

    # Separate the dataframes based on the 'Topic' column
    df0_normal_m = df0_m[df0_m['Topic'] == 'topic_normal']
    df0_priority_m = df0_m[df0_m['Topic'] == 'topic_priority']

    df1_normal_m = df1_m[df1_m['Topic'] == 'topic_normal']
    df1_priority_m = df1_m[df1_m['Topic'] == 'topic_priority']

    df2_normal_m = df2_m[df2_m['Topic'] == 'topic_normal']
    df2_priority_m = df2_m[df2_m['Topic'] == 'topic_priority']

    df3_normal_m = df3_m[df3_m['Topic'] == 'topic_normal']
    df3_priority_m = df3_m[df3_m['Topic'] == 'topic_priority']

    # # Normalize all dataframes to the same elapsed time
    max_time_normal_m = min(df0_normal_m['Time'].max(), df1_normal_m['Time'].max(), df2_normal_m['Time'].max(), df3_normal_m['Time'].max())
    max_time_priority_m = min(df0_priority_m['Time'].max(), df1_priority_m['Time'].max(), df2_priority_m['Time'].max(), df3_priority_m['Time'].max())

    df0_normal_m = df0_normal_m[df0_normal_m['Time'] <= max_time_normal_m]
    df1_normal_m = df1_normal_m[df1_normal_m['Time'] <= max_time_normal_m]
    df2_normal_m = df2_normal_m[df2_normal_m['Time'] <= max_time_normal_m]
    df3_normal_m = df3_normal_m[df3_normal_m['Time'] <= max_time_normal_m]

    df0_priority_m = df0_priority_m[df0_priority_m['Time'] <= max_time_priority_m]
    df1_priority_m = df1_priority_m[df1_priority_m['Time'] <= max_time_priority_m]
    df2_priority_m = df2_priority_m[df2_priority_m['Time'] <= max_time_priority_m]
    df3_priority_m = df3_priority_m[df3_priority_m['Time'] <= max_time_priority_m]

    # Calculate the average throughput over the different FileId's
    df0_normal_avg_m = df0_normal_m.groupby('Time').Throughput.mean().reset_index()
    df1_normal_avg_m = df1_normal_m.groupby('Time').Throughput.mean().reset_index()
    df2_normal_avg_m = df2_normal_m.groupby('Time').Throughput.mean().reset_index()
    df3_normal_avg_m = df3_normal_m.groupby('Time').Throughput.mean().reset_index()

    df0_priority_avg_m = df0_priority_m.groupby('Time').Throughput.mean().reset_index()
    df1_priority_avg_m = df1_priority_m.groupby('Time').Throughput.mean().reset_index()
    df2_priority_avg_m = df2_priority_m.groupby('Time').Throughput.mean().reset_index()
    df3_priority_avg_m = df3_priority_m.groupby('Time').Throughput.mean().reset_index()

    plt.figure(figsize=(12, 8))
    sns.set_context("notebook")
    # sns.set_style("whitegrid")

    colors = sns.color_palette()

    df0_normal_avg_m = df0_normal_avg_m[df0_normal_avg_m['Time'] <= 200]
    df1_normal_avg_m = df1_normal_avg_m[df1_normal_avg_m['Time'] <= 200]
    df2_normal_avg_m = df2_normal_avg_m[df2_normal_avg_m['Time'] <= 200]
    df3_normal_avg_m = df3_normal_avg_m[df3_normal_avg_m['Time'] <= 200]

    df0_priority_avg_m = df0_priority_avg_m[df0_priority_avg_m['Time'] <= 200]
    df1_priority_avg_m = df1_priority_avg_m[df1_priority_avg_m['Time'] <= 200]
    df2_priority_avg_m = df2_priority_avg_m[df2_priority_avg_m['Time'] <= 200]
    df3_priority_avg_m = df3_priority_avg_m[df3_priority_avg_m['Time'] <= 200]

    plt.subplot(2, 1, 1)
    # Plot without labels
    plt.plot(df0_priority_avg_m['Time'], df0_priority_avg_m['Throughput'], color=colors[3], marker='^')
    plt.plot(df1_priority_avg_m['Time'], df1_priority_avg_m['Throughput'], color=colors[3], marker='o')
    plt.plot(df2_priority_avg_m['Time'], df2_priority_avg_m['Throughput'], color=colors[3], marker='s')
    plt.plot(df3_priority_avg_m['Time'], df3_priority_avg_m['Throughput'], color=colors[3], marker='d')
    plt.plot(df0_normal_avg_m['Time'], df0_normal_avg_m['Throughput'], color=colors[0], marker='^')
    plt.plot(df1_normal_avg_m['Time'], df1_normal_avg_m['Throughput'], color=colors[0], marker='o')
    plt.plot(df2_normal_avg_m['Time'], df2_normal_avg_m['Throughput'], color=colors[0], marker='s')
    plt.plot(df3_normal_avg_m['Time'], df3_normal_avg_m['Throughput'], color=colors[0], marker='d')

    # Draw a line through 1.20 throughput per second
    plt.axhline(y=1.20, color='g', linestyle='--')

    # Add an arrow annotation saying the data points just below the threshold have a value of 1.19 in the middle of the graph
    plt.annotate('1.19', xy=(100, 1.15), xytext=(100, 0.79), arrowprops=dict(facecolor='black', shrink=0.05), horizontalalignment='center')
    plt.annotate('1.19', xy=(20, 1.15), xytext=(20, 0.79), arrowprops=dict(facecolor='black', shrink=0.05), horizontalalignment='center')
    plt.annotate('1.19', xy=(30, 1.15), xytext=(30, 0.79), arrowprops=dict(facecolor='black', shrink=0.05), horizontalalignment='center')
    plt.annotate('1.19', xy=(140, 1.15), xytext=(140, 0.79), arrowprops=dict(facecolor='black', shrink=0.05), horizontalalignment='center')

    # Add an text annotation at the threshold line saying "1.20". Make the font bold.
    plt.annotate('1.20', xy=(-1.5, 1.22), xytext=(-1.5, 1.22), horizontalalignment='right', verticalalignment='bottom', fontweight='bold')



    plt.xlabel('Time [seconds]')
    plt.ylabel('Average Message Throughput / second')
    plt.grid()

    # Custom legend entries
    from matplotlib.lines import Line2D
    legend_elements = [Line2D([0], [0], color=colors[0], label='Normal Events'),
                    Line2D([0], [0], color=colors[3], label='Priority Events'),
                    Line2D([0], [0], color='g', linestyle='--', label='Throughput Threshold'),
                    Line2D([0], [0], marker='d', color='w', markerfacecolor='gray', markersize=10, label='No policy'),
                    Line2D([0], [0], marker='^', color='w', markerfacecolor='gray', markersize=10, label='Simple policy'),
                    Line2D([0], [0], marker='o', color='w', markerfacecolor='gray', markersize=10, label='Moving Average Policy'),
                    Line2D([0], [0], marker='s', color='w', markerfacecolor='gray', markersize=10, label='Exponential smoothing policy'),
                    ]


    plt.legend(handles=legend_elements, loc='lower right')
    plt.tight_layout()
    plt.savefig(f's_{workload}_p0-3_message_throughput.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)


    # Create the subplots
    # plt.figure(figsize=(12, 8))
    # plt.subplot(2, 1, 1)

    # plt.xlabel('Time (seconds)')
    # plt.ylabel('Average Message Throughput')
    # plt.grid()
    # plt.legend()
    # plt.tight_layout()
    # plt.savefig(f'e1/s_{workload}_p0-1_normal_message_throughput.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)
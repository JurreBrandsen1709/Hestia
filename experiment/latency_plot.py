import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def process_df(path, config_priority, config_normal):
    df = pd.read_csv(path)
    df['Time'] = pd.to_datetime(df['Time'])
    df = df[df['Latency'] > 0]
    df = df[df['Latency'] < 100]

    df_priority = df[df['Topic'] == 'topic_priority']
    df_normal = df[df['Topic'] == 'topic_normal']

    df_priority['Configuration'] = config_priority
    df_normal['Configuration'] = config_normal

    return df_priority, df_normal

workloads = ['p1']

for workload in workloads:

    # Define the file paths and corresponding configurations
    paths_configs = [
        (f'star_topology/s_w1_{workload}_latency.csv', 'TCP', 'TCP'),
        (f'star_topology/s_w2_{workload}_latency.csv', 'MA', 'MA'),
        (f'star_topology/s_w3_{workload}_latency.csv', 'ES', 'ES'),
    ]

    all_data_priority = pd.DataFrame()
    all_data_normal = pd.DataFrame()

    for path, config_priority, config_normal in paths_configs:
        df_priority, df_normal = process_df(path, config_priority, config_normal)
        all_data_priority = pd.concat([all_data_priority, df_priority])
        all_data_normal = pd.concat([all_data_normal, df_normal])

    # Plot for Priority Topic
    plt.figure(figsize=(8, 6))
    sns.boxplot(x="Latency", y="Configuration", data=all_data_priority, palette=sns.color_palette())
    plt.xlim(left=0)
    plt.xlabel('Latency (ms)')
    plt.ylabel('')

    plt.tight_layout()
    plt.savefig(f'e3/s_w1-3_{workload}_latency_priority.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)

    # Plot for Normal Topic
    plt.figure(figsize=(8, 6))
    sns.boxplot(x="Latency", y="Configuration", data=all_data_normal, palette=sns.color_palette())
    plt.xlim(left=0)
    plt.xlabel('Latency (ms)')
    plt.ylabel('')

    plt.tight_layout()
    plt.savefig(f'e3/s_w1-3_{workload}_latency_normal.pdf', bbox_inches='tight', pad_inches=0.05, dpi=300)

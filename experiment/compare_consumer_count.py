import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def load_and_process_data(file_path):
    df = pd.read_csv(file_path)
    df['Time'] = pd.to_datetime(df['Time'])
    return df

def calculate_time_diff(df):
    df = df.sort_values(['Topic', 'FileId', 'ConsumerCount'])
    df['TimeDiff'] = df.groupby(['Topic', 'ConsumerCount'])['Time'].diff().dt.total_seconds()
    return df

def plot_time_diff(df1, df2):
    topics = df1['Topic'].unique()
    fig, axes = plt.subplots(len(topics), 2, figsize=(20, 10*len(topics)))
    fig.tight_layout(pad=10.0)

    for i, topic in enumerate(topics):
        time_diff1 = df1[df1['Topic'] == topic]['TimeDiff'].dropna()
        time_diff2 = df2[df2['Topic'] == topic]['TimeDiff'].dropna()

        sns.histplot(time_diff1, ax=axes[i, 0])
        axes[i, 0].set_title(f'Time differences for {topic} - df1', fontsize=16)
        axes[i, 0].set_xlabel('Time difference (seconds)', fontsize=14)
        axes[i, 0].set_ylabel('Count', fontsize=14)

        sns.histplot(time_diff2, ax=axes[i, 1])
        axes[i, 1].set_title(f'Time differences for {topic} - df2', fontsize=16)
        axes[i, 1].set_xlabel('Time difference (seconds)', fontsize=14)
        axes[i, 1].set_ylabel('Count', fontsize=14)

    plt.show()

def main():
    df1 = load_and_process_data('e1_normal_consumer_count.csv')
    df2 = load_and_process_data('e1_dyconits_consumer_count.csv')
    df1 = calculate_time_diff(df1)
    df2 = calculate_time_diff(df2)
    plot_time_diff(df1, df2)

if __name__ == "__main__":
    main()
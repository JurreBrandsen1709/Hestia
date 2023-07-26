import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def calculate_consumer_lag(data):
    data['Time'] = pd.to_datetime('2000-01-01 ' + data['Time'])
    grouped_data = data.groupby(['ConsumerCount', 'Topic']).count()
    filtered_data = grouped_data[grouped_data['FileId'] == 4].reset_index()
    filtered_df = pd.merge(data, filtered_data[['ConsumerCount', 'Topic']], how='inner', on=['ConsumerCount', 'Topic'])
    grouped_df = filtered_df.groupby(['ConsumerCount', 'Topic'])
    consumer_lag_df = pd.DataFrame()
    for name, group in grouped_df:
        file_ids = group['FileId'].unique()
        for file_id in file_ids:
            time_diffs = []
            time1 = group[group['FileId'] == file_id]['Time'].values[0]
            for other_file_id in file_ids:
                if other_file_id != file_id:
                    time2 = group[group['FileId'] == other_file_id]['Time'].values[0]
                    time_diff = np.abs((time2 - time1) / np.timedelta64(1, 's'))
                    time_diffs.append(time_diff)
            average_time_diff = np.mean(time_diffs)
            consumer_lag_df = pd.concat([consumer_lag_df, pd.DataFrame({
                'ConsumerCount': name[0],
                'Topic': name[1],
                'FileId': file_id,
                'ConsumerLag': average_time_diff
            }, index=[0])], ignore_index=True)
    return consumer_lag_df

# Load the data
data1 = pd.read_csv('e1_w2/normal_consumer_count.csv')
data2 = pd.read_csv('e1_w2/p1_consumer_count.csv')
data3 = pd.read_csv('e1_w2/p2_consumer_count.csv')
data4 = pd.read_csv('e1_w2/p3_consumer_count.csv')

# Calculate consumer lag for all three datasets
consumer_lag_df1 = calculate_consumer_lag(data1)
consumer_lag_df2 = calculate_consumer_lag(data2)
consumer_lag_df3 = calculate_consumer_lag(data3)
consumer_lag_df4 = calculate_consumer_lag(data4)

# Calculate the average consumer lag per message for each scenario
average_lag_per_message1 = consumer_lag_df1.groupby(['ConsumerCount', 'Topic'])['ConsumerLag'].mean().reset_index()
average_lag_per_message2 = consumer_lag_df2.groupby(['ConsumerCount', 'Topic'])['ConsumerLag'].mean().reset_index()
average_lag_per_message3 = consumer_lag_df3.groupby(['ConsumerCount', 'Topic'])['ConsumerLag'].mean().reset_index()
average_lag_per_message4 = consumer_lag_df4.groupby(['ConsumerCount', 'Topic'])['ConsumerLag'].mean().reset_index()

# Create separate dataframes for 'topic_normal' and 'topic_priority' for all datasets
average_lag_normal1 = average_lag_per_message1[average_lag_per_message1['Topic'] == 'topic_normal']
average_lag_priority1 = average_lag_per_message1[average_lag_per_message1['Topic'] == 'topic_priority']
average_lag_normal2 = average_lag_per_message2[average_lag_per_message2['Topic'] == 'topic_normal']
average_lag_priority2 = average_lag_per_message2[average_lag_per_message2['Topic'] == 'topic_priority']
average_lag_normal3 = average_lag_per_message3[average_lag_per_message3['Topic'] == 'topic_normal']
average_lag_priority3 = average_lag_per_message3[average_lag_per_message3['Topic'] == 'topic_priority']
average_lag_normal4 = average_lag_per_message4[average_lag_per_message4['Topic'] == 'topic_normal']
average_lag_priority4 = average_lag_per_message4[average_lag_per_message4['Topic'] == 'topic_priority']


# Create a line plot for average consumer lag per message for 'topic_normal'
plt.figure(figsize=(10, 6))
plt.plot(average_lag_normal1['ConsumerCount'], average_lag_normal1['ConsumerLag'], label='Without synchronization')
plt.plot(average_lag_normal2['ConsumerCount'], average_lag_normal2['ConsumerLag'], label='With reverseTCP synchronization')
plt.plot(average_lag_normal3['ConsumerCount'], average_lag_normal3['ConsumerLag'], label='With moving average synchronization')
plt.plot(average_lag_normal4['ConsumerCount'], average_lag_normal4['ConsumerLag'], label='With exponential smoothing synchronization')
plt.title('Average Consumer Lag per Message for topic_normal')
plt.xlabel('ConsumerCount')
plt.ylabel('Average Consumer Lag (seconds)')
plt.legend()
plt.show()

# Create a line plot for average consumer lag per message for 'topic_priority'
plt.figure(figsize=(10, 6))
plt.plot(average_lag_priority1['ConsumerCount'], average_lag_priority1['ConsumerLag'], label='Without synchronization')
plt.plot(average_lag_priority2['ConsumerCount'], average_lag_priority2['ConsumerLag'], label='With reverseTCP synchronization')
plt.plot(average_lag_priority3['ConsumerCount'], average_lag_priority3['ConsumerLag'], label='With moving average synchronization')
plt.plot(average_lag_priority4['ConsumerCount'], average_lag_priority4['ConsumerLag'], label='With exponential smoothing synchronization')
plt.title('Average Consumer Lag per Message for topic_priority')
plt.xlabel('ConsumerCount')
plt.ylabel('Average Consumer Lag (seconds)')
plt.legend()
plt.show()


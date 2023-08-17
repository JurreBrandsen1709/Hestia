import pandas as pd

df = pd.read_csv('t_w2_p0_consumer_count.csv')
df = df[~((df['Topic'].str.contains('trans')) & (df['ConsumerCount'] % 2 == 1))]

df['ConsumerCount'] = df['ConsumerCount'].astype(int)
df['ConsumerCount'] = df['ConsumerCount'].apply(lambda x: x // 2)

# save to csv
df.to_csv('t_w2_p0_consumer_count.csv', index=False)

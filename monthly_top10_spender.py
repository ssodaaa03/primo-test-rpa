import pandas as pd

usecols = ['customer_no', 'amount', 'transaction_datetime']
df = pd.read_csv('10Mrecord.csv', usecols=usecols)

df['transaction_datetime'] = pd.to_datetime(df['transaction_datetime'], errors='coerce')
df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
df = df.dropna(subset=['transaction_datetime', 'customer_no', 'amount'])

df['month'] = df['transaction_datetime'].dt.to_period('M')

agg = (df.groupby(['month', 'customer_no'], as_index=False)['amount']
         .sum()
         .rename(columns={'amount': 'total_amount'}))

top10 = (agg.sort_values(['month', 'total_amount'], ascending=[True, False])
            .groupby('month', as_index=False)
            .head(10))

top10['rank'] = (top10.groupby('month')['total_amount']
                       .rank(method='first', ascending=False)
                       .astype(int))

top10['month'] = top10['month'].dt.month.astype(str).str.zfill(2)

top10 = top10[['month', 'rank', 'customer_no', 'total_amount']].sort_values(['month', 'rank'])

top10.to_csv('top_spenders_by_month.csv', index=False)
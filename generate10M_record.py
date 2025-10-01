import numpy as np
import pandas as pd

df = pd.read_csv('large_order_data_2024.csv', index_col=False)


def gen_orders(n, start='2024-01-01 00:00:00', end='2024-12-31 23:59:59', existing_df=None):
    rng = np.random.default_rng()

    branches = np.array(['BANGKOK','CHIANGMAI','KHONKAEN','PHUKET','HATYAI'])
    brands   = np.array(['NIKE','ADIDAS','PUMA','REEBOK','NEWBALANCE'])
    sku_prefix = {'NIKE':'NI','ADIDAS':'AD','PUMA':'PU','REEBOK':'RE','NEWBALANCE':'NE'}
    cats = np.array(['SHOE','BAG','PANT','CAP','SOCK'])

    start_ts = pd.Timestamp(start)
    end_ts   = pd.Timestamp(end)
    total_sec = int((end_ts - start_ts).total_seconds())
    offsets = rng.integers(0, total_sec + 1, size=n)
    tx_dt   = start_ts + pd.to_timedelta(offsets, unit='s')

    amount  = rng.integers(100, 10001, size=n)
    qty     = rng.integers(1, 7, size=n)
    branch  = rng.choice(branches, size=n)
    brand   = rng.choice(brands,   size=n)
    customer = np.array([f"CUST{v:05d}" for v in rng.integers(1, 500_001, size=n)])

    sku_nums   = rng.integers(1, 100, size=n)
    brand_pref = np.fromiter((sku_prefix[b] for b in brand), dtype='U12', count=n)
    cat        = rng.choice(cats, size=n)
    sku = np.array([f"{p}-{c}-{num:02d}" for p, c, num in zip(brand_pref, cat, sku_nums)])

    if existing_df is not None and 'order_no' in existing_df.columns:
        m = existing_df['order_no'].astype(str).str.extract(r'(\d+)')
        base = m[0].dropna().astype(int).max() if not m.empty else 0
    else:
        base = 0
    order_no = np.array([f"ORD{base + i + 1}" for i in range(n)])

    new_df = pd.DataFrame({
        'order_no': order_no,
        'amount': amount,
        'customer_no': customer,
        'branch': branch,
        'brand': brand,
        'sku': sku,
        'quantity': qty,
        'transaction_datetime': tx_dt.strftime('%Y-%m-%d %H:%M:%S')
    })

    return new_df

new_rows = gen_orders(9000000, existing_df=df)
new_rows = new_rows.reindex(columns=df.columns)
df = pd.concat([df, new_rows], ignore_index=True)

df.to_csv('large_order_data_2024_with_new_rows.csv', index=False)
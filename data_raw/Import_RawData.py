import pandas as pd
import clickhouse_connect
data = pd.read_csv('batdongsan_merged.csv')
client = clickhouse_connect.get_client(
    host="lbanrxx268.ap-southeast-1.aws.clickhouse.cloud",
    port=8443,
    username="default",
    password="~WfW3boz~nJtl",
    secure=True
)
data['ngay_dang'] = pd.to_datetime(data['ngay_dang'], dayfirst=True)
data['ma_tin'] = data['ma_tin'].astype(int)
data = data.where(pd.notna(data), None)
# Insert dữ liệu vào ClickHouse
records = [tuple(str(row) if isinstance(row, float) else row for row in r) for r in data.itertuples(index=False, name=None)]
client.insert("batdongsan", records, column_names=list(data.columns))
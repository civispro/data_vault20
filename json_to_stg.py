import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from hashlib import sha1


user = "root"
password = "root"
host = "0.0.0.0"
port = 5432
db = "stg_db"

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
engine.connect()

now = datetime.now()

source = "https://jsonplaceholder.typicode.com/posts/"
df = pd.read_json(source)

df.rename(columns={"userId": "bk_user_id", "id": "bk_post_id"},inplace=True)
df["load_date"] = now
df['bk_post_id_hash_key'] = df.apply(lambda x: sha1(str(x["bk_post_id"]).lower().encode('utf-8')).hexdigest(), axis=1)
df["bk_user_id_hash_key"] = df.apply(lambda x: sha1(str(x["bk_user_id"]).lower().encode('utf-8')).hexdigest(), axis=1)

engine.execute("TRUNCATE stg.posts")
df.to_sql(name='posts', schema='stg', con=engine, index=False, if_exists='append', method='multi')
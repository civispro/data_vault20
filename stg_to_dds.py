from sqlalchemy import create_engine

user = "root"
password = "root"
host = "0.0.0.0"
port = 5432
db = "dds_db"

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

with engine.begin() as conn:
   conn.execute("Select dds.stg_to_dds()")



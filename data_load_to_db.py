import psycopg2
import os
from dotenv import load_dotenv


load_dotenv()
conn = psycopg2.connect(
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host="localhost",
    port="5433"
)
cur = conn.cursor()

cur.execute('CREATE SCHEMA IF NOT EXISTS stat;')
cur.execute('''CREATE TABLE IF NOT EXISTS stat.daily_ibm (
    date DATE PRIMARY KEY,
    open NUMERIC(10,4),
    high NUMERIC(10,4),
    low NUMERIC(10,4),
    close NUMERIC(10,4),
    volume BIGINT
);''')
with open("daily_ibm.csv", "r") as f:
    next(f)  # пропустить заголовок
    cur.execute('TRUNCATE TABLE stat.daily_ibm;')
    cur.copy_expert("COPY stat.daily_ibm FROM STDIN WITH CSV HEADER", f)

conn.commit()
cur.close()
conn.close()
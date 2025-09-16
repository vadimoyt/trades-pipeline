import csv
import os

import psycopg2
import requests
from dotenv import load_dotenv


def data_load_to_db():
    load_dotenv()
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host="trades-pipeline-postgres-1",
        port="5432"
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


def data_extraction():
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo'
    data = requests.get(url).json()

    first_rows = next(iter(data['Time Series (Daily)'].values()))
    fieldnames = ['date'] + [v.split('. ')[1] for v in first_rows]

    with open('daily_ibm.csv', 'w', newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        for date, values in data['Time Series (Daily)'].items():
            row = {'date': date}
            row.update({key.split(". ")[1]: val for key, val in values.items()})
            writer.writerow(row)

    return  os.getcwd()
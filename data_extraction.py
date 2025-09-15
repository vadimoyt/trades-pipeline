import json
import csv
import requests


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

import json
import pendulum
import requests
from datetime import datetime,timedelta
import pandas as pd
from pymongo import MongoClient
from bs4 import BeautifulSoup

from airflow import Dataset
from airflow.decorators import dag, task

symbols = ['AAPL', 'IBM', 'AMZN', 'MSFT', 'TSLA'] 
client = MongoClient("mongodb+srv://test:test@cluster0.kynbj2b.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client.get_database("finance_metadata")
now = pendulum.now()


@dag(start_date=now, schedule="@daily", catchup=False)
def etl_finance_meta():
    @task()
    def extract():
        data_finance = {}
        for symbol in symbols:
            url = f'https://www.alphavantage.co/query'
            params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'outputsize':'full',  
            'apikey': "YGDVY075AOQGC0BE"
            }
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if 'Time Series (Daily)' in data:
                    data_finance[symbol] = data['Time Series (Daily)']
                else:
                    print(f"No intraday data available for {symbol}")
                    return None
            else:
                print(f"Failed to retrieve intraday data for {symbol}")
                return None
        return data_finance
    @task()
    def get_company_metadata():
        meta = {}
        for symbol in symbols:
            url = f'https://finance.yahoo.com/quote/{symbol}/'
            response = requests.get(url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                metadata = {}
                company_name = soup.find('h1', class_='D(ib) Fz(18px)').text.strip()
                metadata['Company Name'] = company_name
                summary_table = soup.find('table', class_='W(100%) M(0) Bdcl(c)')
                if summary_table:
                    rows = summary_table.find_all('tr')
                    for row in rows:
                        cells = row.find_all('td')
                        if len(cells) == 2:
                            label = cells[0].text.strip()
                            value = cells[1].text.strip()
                            metadata[label] = value
                meta[symbol] = metadata
            else:
                print(f"Failed to retrieve metadata for {symbol}")
                meta[symbol] = None
        return meta

    @task()
    def rename_columns(data: json):
        rename = {}
        for key in data.keys():
            renamed_data = {}
            for date_str, value in data[key].items():
                renamed_value = {
                    'open': value['1. open'],
                    'high': value['2. high'],
                    'low': value['3. low'],
                    'close': value['4. close'],
                    'volume': value['5. volume']
                }
                renamed_data[date_str] = renamed_value
            rename[key] =  renamed_data
        return rename
    
    @task()
    def join_data(alpha_vantage_data, yahoo_metadata):
        if alpha_vantage_data is None or yahoo_metadata is None:
            return None
        
        joined={}
        for symbol in symbols:
            joined_data = {}
            for timestamp, value in alpha_vantage_data[symbol].items():
                joined_entry = {}
                joined_entry.update(value)
                joined_entry.update(yahoo_metadata[symbol])
                joined_data[timestamp] = joined_entry
            
            joined[symbol]= joined_data
        return joined



    @task()
    def clean_data(data: json):
        clean = {}
        for key in data.keys():
            cleaned_data = {}
            for date_str, value in data[key].items():
                if all(key1 in value for key1 in ['1. open', '2. high', '3. low', '4. close', '5. volume']):
                    cleaned_data[date_str] = value
            clean[key] =  cleaned_data
        return clean

    @task()
    def transform_weekly(data: json):
        def group_data_by_weeks(data):
            grouped={}
            for symbol in data.keys():
                grouped_data = {}
                for date_str, value in data[symbol].items():
                    date = datetime.strptime(date_str, '%Y-%m-%d')
                    week_start = date - timedelta(days=date.weekday())
                    week_end = week_start + timedelta(days=6)
                    week_label = f"{week_start.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')}"
                    if week_label not in grouped_data:
                        grouped_data[week_label] = []
                    grouped_data[week_label].append((date, value))
                grouped[symbol]= grouped_data
            return grouped
        def calculate_weekly_summary(week_data):
            weekly_open = float(week_data[-1][1]['open'])
            weekly_close = float(week_data[0][1]['close'])
            weekly_volume = sum(float(entry[1]['volume']) for entry in week_data)
            weekly_high = max(float(entry[1]['high']) for entry in week_data)
            weekly_low = min(float(entry[1]['low']) for entry in week_data)
            comp_name = week_data[0][1]['Company Name']
            market_cap = week_data[0][1]['Market Cap']
            beta_5yr = week_data[0][1]['Beta (5Y Monthly)']
            pe_ratio = week_data[0][1]['PE Ratio (TTM)']
            eps = week_data[0][1]['EPS (TTM)']
            earning_date = week_data[0][1]['Earnings Date']
            fwd_div = week_data[0][1]['Forward Dividend & Yield']
            ex_div = week_data[0][1]['Ex-Dividend Date']
            tar_1y = week_data[0][1]['1y Target Est']
            return {
                'open': weekly_open,
                'high': weekly_high,
                'low': weekly_low,
                'close': weekly_close,
                'volume': weekly_volume,
                'Company Name': comp_name,
                'Market Cap': market_cap,
                'Beta (5Y Monthly)': beta_5yr,
                'PE Ratio (TTM)': pe_ratio,
                'EPS (TTM)': eps,
                'Earnings Date': earning_date,
                'Forward Dividend & Yield': fwd_div,
                'Ex-Dividend Date': ex_div,
                '1y Target Est': tar_1y
            }
        weekly_data = group_data_by_weeks(data)
        transformed_data = []
        for symbol in weekly_data.keys():

            for week_label, week_data in weekly_data[symbol].items():
                weekly_summary = calculate_weekly_summary(week_data)
                dat =  {
                    'Date': week_label,
                    'Symbol': symbol,
                    'Open': weekly_summary['open'],
                    'High': weekly_summary['high'],
                    'Close': weekly_summary['close'],
                    'Low': weekly_summary['low'],
                    'Volume': weekly_summary['volume'],
                    'Company Name': weekly_summary['Company Name'],
                    'Market Cap': weekly_summary['Market Cap'],
                    'Beta (5Y Monthly)': weekly_summary['Beta (5Y Monthly)'],
                    'PE Ratio (TTM)':weekly_summary['PE Ratio (TTM)'],
                    'EPS (TTM)': weekly_summary['EPS (TTM)'],
                    'Earnings Date': weekly_summary['Earnings Date'],
                    'Forward Dividend & Yield': weekly_summary['Forward Dividend & Yield'],
                    'Ex-Dividend Date': weekly_summary['Ex-Dividend Date'],
                    '1y Target Est': weekly_summary['1y Target Est']

                }
                transformed_data.append(dat)
        return transformed_data

    @task()
    def transform_monthly(data: json):
        def group_data_by_months(data):
            grouped = {}
            for symbol in data.keys():
                grouped_data = {}
                for date_str, value in data[symbol].items():
                    date = datetime.strptime(date_str, '%Y-%m-%d')
                    month_start = date.replace(day=1)
                    month_end = month_start.replace(day=1, month=month_start.month % 12 + 1) - timedelta(days=1)
                    month_label = f"{month_start.strftime('%Y-%m-%d')} to {month_end.strftime('%Y-%m-%d')}"
                    if month_label not in grouped_data:
                        grouped_data[month_label] = []
                    grouped_data[month_label].append((date, value))
                grouped[symbol] = grouped_data
            return grouped

        def calculate_monthly_summary(month_data):
            monthly_open = float(month_data[-1][1]['open'])
            monthly_close = float(month_data[0][1]['close'])
            monthly_volume = sum(float(entry[1]['volume']) for entry in month_data)
            monthly_high = max(float(entry[1]['high']) for entry in month_data)
            monthly_low = min(float(entry[1]['low']) for entry in month_data)
            comp_name = month_data[0][1]['Company Name']
            market_cap = month_data[0][1]['Market Cap']
            beta_5yr = month_data[0][1]['Beta (5Y Monthly)']
            pe_ratio = month_data[0][1]['PE Ratio (TTM)']
            eps = month_data[0][1]['EPS (TTM)']
            earning_date = month_data[0][1]['Earnings Date']
            fwd_div = month_data[0][1]['Forward Dividend & Yield']
            ex_div = month_data[0][1]['Ex-Dividend Date']
            tar_1y = month_data[0][1]['1y Target Est']
            return {
                'open': monthly_open,
                'high': monthly_high,
                'low': monthly_low,
                'close': monthly_close,
                'volume': monthly_volume,
                'Company Name': comp_name,
                'Market Cap': market_cap,
                'Beta (5Y Monthly)': beta_5yr,
                'PE Ratio (TTM)': pe_ratio,
                'EPS (TTM)': eps,
                'Earnings Date': earning_date,
                'Forward Dividend & Yield': fwd_div,
                'Ex-Dividend Date': ex_div,
                '1y Target Est': tar_1y
            }

        monthly_data = group_data_by_months(data)
        transformed_data = []
        for symbol in monthly_data.keys():
            for month_label, month_data in monthly_data[symbol].items():
                monthly_summary = calculate_monthly_summary(month_data)
                dat = {
                    'Date': month_label,
                    'Symbol': symbol,
                    'Open': monthly_summary['open'],
                    'High': monthly_summary['high'],
                    'Close': monthly_summary['close'],
                    'Low': monthly_summary['low'],
                    'Volume': monthly_summary['volume'],
                    'Company Name': monthly_summary['Company Name'],
                    'Market Cap': monthly_summary['Market Cap'],
                    'Beta (5Y Monthly)': monthly_summary['Beta (5Y Monthly)'],
                    'PE Ratio (TTM)':monthly_summary['PE Ratio (TTM)'],
                    'EPS (TTM)': monthly_summary['EPS (TTM)'],
                    'Earnings Date': monthly_summary['Earnings Date'],
                    'Forward Dividend & Yield': monthly_summary['Forward Dividend & Yield'],
                    'Ex-Dividend Date': monthly_summary['Ex-Dividend Date'],
                    '1y Target Est': monthly_summary['1y Target Est']
                }
                transformed_data.append(dat)
        return transformed_data

    
    @task()
    def transform_yearly(data: json):
        def group_data_by_years(data):
            grouped = {}
            for symbol in data.keys():
                grouped_data = {}
                for date_str, value in data[symbol].items():
                    date = datetime.strptime(date_str, '%Y-%m-%d')
                    year_start = date.replace(month=1, day=1)
                    year_end = year_start.replace(month=12, day=31)
                    year_label = f"{year_start.strftime('%Y-%m-%d')} to {year_end.strftime('%Y-%m-%d')}"
                    if year_label not in grouped_data:
                        grouped_data[year_label] = []
                    grouped_data[year_label].append((date, value))
                grouped[symbol] =  grouped_data
            return grouped

        def calculate_yearly_summary(year_data):
            yearly_open = float(year_data[-1][1]['open'])
            yearly_close = float(year_data[0][1]['close'])
            yearly_volume = sum(float(entry[1]['volume']) for entry in year_data)
            yearly_high = max(float(entry[1]['high']) for entry in year_data)
            yearly_low = min(float(entry[1]['low']) for entry in year_data)
            comp_name = year_data[0][1]['Company Name']
            market_cap = year_data[0][1]['Market Cap']
            beta_5yr = year_data[0][1]['Beta (5Y Monthly)']
            pe_ratio = year_data[0][1]['PE Ratio (TTM)']
            eps = year_data[0][1]['EPS (TTM)']
            earning_date = year_data[0][1]['Earnings Date']
            fwd_div = year_data[0][1]['Forward Dividend & Yield']
            ex_div = year_data[0][1]['Ex-Dividend Date']
            tar_1y = year_data[0][1]['1y Target Est']
            return {
                'open': yearly_open,
                'high': yearly_high,
                'low': yearly_low,
                'close': yearly_close,
                'volume': yearly_volume,
                'Company Name': comp_name,
                'Market Cap': market_cap,
                'Beta (5Y Monthly)': beta_5yr,
                'PE Ratio (TTM)': pe_ratio,
                'EPS (TTM)': eps,
                'Earnings Date': earning_date,
                'Forward Dividend & Yield': fwd_div,
                'Ex-Dividend Date': ex_div,
                '1y Target Est': tar_1y
            }

        yearly_data = group_data_by_years(data)
        transformed_data = []
        for symbol in yearly_data.keys():
            for year_label, year_data in yearly_data[symbol].items():
                yearly_summary = calculate_yearly_summary(year_data)
                dat = {
                    'Date': year_label,
                    'Symbol': symbol,
                    'Open': yearly_summary['open'],
                    'High': yearly_summary['high'],
                    'Close': yearly_summary['close'],
                    'Low': yearly_summary['low'],
                    'Volume': yearly_summary['volume'],
                    'Company Name': yearly_summary['Company Name'],
                    'Market Cap': yearly_summary['Market Cap'],
                    'Beta (5Y Monthly)': yearly_summary['Beta (5Y Monthly)'],
                    'PE Ratio (TTM)':yearly_summary['PE Ratio (TTM)'],
                    'EPS (TTM)': yearly_summary['EPS (TTM)'],
                    'Earnings Date': yearly_summary['Earnings Date'],
                    'Forward Dividend & Yield': yearly_summary['Forward Dividend & Yield'],
                    'Ex-Dividend Date': yearly_summary['Ex-Dividend Date'],
                    '1y Target Est': yearly_summary['1y Target Est']
                }
                transformed_data.append(dat)
        return transformed_data

    @task()
    def transform_daily(data: json):
        formatted_data = []
        for symbol in data.keys():
            for date, data_point in data[symbol].items():
                formatted_data.append(
                        {
                            'Date': date,
                            'Symbol': symbol,
                            'Open': float(data_point['open']),
                            'High': float(data_point['high']),
                            'Low': float(data_point['low']),
                            'Close': float(data_point['close']),
                            'Volume': int(data_point['volume']),
                            'Company Name': data_point['Company Name'],
                            'Market Cap': data_point['Market Cap'],
                            'Beta (5Y Monthly)': data_point['Beta (5Y Monthly)'],
                            'PE Ratio (TTM)':data_point['PE Ratio (TTM)'],
                            'EPS (TTM)': data_point['EPS (TTM)'],
                            'Earnings Date': data_point['Earnings Date'],
                            'Forward Dividend & Yield': data_point['Forward Dividend & Yield'],
                            'Ex-Dividend Date': data_point['Ex-Dividend Date'],
                            '1y Target Est': data_point['1y Target Est']
                        }
                )
        return formatted_data

    @task()
    def load_daily(data: json):
        records = db.stocks_daily
        records.delete_many({})
        records.insert_many(data)

    @task()
    def load_weekly(data: json):
        records = db.stocks_weekly
        records.delete_many({})
        records.insert_many(data)

    @task()
    def load_monthly(data: json):
        records = db.stocks_monthly
        records.delete_many({})
        records.insert_many(data)

    @task()
    def load_yearly(data: json):
        records = db.stocks_yearly
        records.delete_many({})
        records.insert_many(data)

    daily_data = extract()
    cleaned_data=clean_data(daily_data)
    renamed_columns=rename_columns(cleaned_data)
    get_metadata = get_company_metadata()
    final_data = join_data(renamed_columns,get_metadata)
    daily_data_transformed=transform_daily(final_data)
    load_daily(daily_data_transformed)
    weekly_data = transform_weekly(final_data)
    load_weekly(weekly_data)
    monthly_data= transform_monthly(final_data)
    load_monthly(monthly_data)
    yearly_data=transform_yearly(final_data) 
    load_yearly(yearly_data)
    
    


etl_finance_meta()
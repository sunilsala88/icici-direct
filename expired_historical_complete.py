from datetime import datetime, timedelta  # Importing necessary modules
import pandas as pd
import time as tt
import sqlite3
import logging

start_time_before=datetime.now()

import credentials as cs
api_key = cs.api_key
secret_key = cs.secret_key
with open('access.txt', 'r') as file:
    session_token = file.read().strip()

expiry_address='banknifty_expiry.csv'
data_base_name='new_bank_nifty_jan_june2.db'
index_name='CNXBAN'
otm_buffer=500
strike_difference=100
past_days=9
start=datetime(2024, 1, 1)
end=datetime(2024, 2, 25)

logging.basicConfig(filename='option_update.log', filemode='w', level=logging.INFO, format=' %(message)s')  # Configuring logging
logging.info('this is my first line')  # Logging the first line

# all_expirys=pd.read_csv(expiry_address)
# all_expirys = pd.to_datetime(all_expirys['Expiry Date']).to_list()

#banknifty
all_expiry=['2024-01-03', '2024-01-10', '2024-01-17', '2024-01-25', '2024-01-31', '2024-02-07', '2024-02-14', '2024-02-21', '2024-02-29', '2024-03-06', '2024-03-13', '2024-03-20', '2024-03-27', '2024-04-03', '2024-04-10', '2024-04-16', '2024-04-24', '2024-04-30', '2024-05-08', '2024-05-15', '2024-05-22', '2024-05-29', '2024-06-05', '2024-06-12', '2024-06-19', '2024-06-26', '2024-06-27', '2024-07-03', '2024-07-10', '2024-07-16', '2024-07-24', '2024-07-31', '2024-08-07', '2024-08-14', '2024-08-21', '2024-08-28', '2024-09-04', '2024-09-11', '2024-09-18', '2024-09-25', '2024-09-26', '2024-10-01', '2024-10-09', '2024-10-16', '2024-10-23', '2024-10-30', '2024-11-06', '2024-11-13', '2024-11-27', '2024-12-24', '2024-12-26', '2025-01-29', '2025-01-30', '2025-02-26', '2025-02-27', '2025-03-26', '2025-03-27']
all_expirys = [pd.to_datetime(x) for x in all_expiry]

#nifty
# all_expiry=[
#     '2024-01-04', '2024-01-11', '2024-01-18', '2024-01-25', '2024-02-01',
#     '2024-02-08', '2024-02-15', '2024-02-22', '2024-02-29', '2024-03-07',
#     '2024-03-14', '2024-03-21', '2024-03-28', '2024-04-04', '2024-04-10',
#     '2024-04-18', '2024-04-25', '2024-05-02', '2024-05-09', '2024-05-16',
#     '2024-05-23', '2024-05-30', '2024-06-06', '2024-06-13', '2024-06-20',
#     '2024-06-27', '2024-07-04', '2024-07-11', '2024-07-18', '2024-07-25',
#     '2024-08-01', '2024-08-08', '2024-08-14', '2024-08-22', '2024-08-29',
#     '2024-09-05', '2024-09-12', '2024-09-19', '2024-09-26', '2024-10-03',
#     '2024-10-10', '2024-10-17', '2024-10-24', '2024-10-31', '2024-11-07',
#     '2024-11-14', '2024-11-21', '2024-11-28', '2024-12-05', '2024-12-12',
#     '2024-12-19', '2024-12-26', '2025-01-02', '2025-01-09', '2025-01-16',
#     '2025-01-23', '2025-01-30', '2025-02-06', '2025-02-13', '2025-02-20',
#     '2025-02-27', '2025-03-06', '2025-03-13', '2025-03-20', '2025-03-27',
#     '2025-04-03', '2025-04-09', '2025-04-17', '2025-04-24', '2025-04-30'
# ]
# all_expirys = [pd.to_datetime(x) for x in all_expiry]


def store_into_database(msg):  # Function to store data into a database
    conn = sqlite3.connect(data_base_name)
    for name, df in msg.items():
        print(name)
        print(df)
        df.to_sql(name, conn, if_exists='replace', index=True)
    conn.commit()



from breeze_connect import BreezeConnect  # Importing BreezeConnect module
breeze = BreezeConnect(api_key=api_key)  # Initializing BreezeConnect SDK

breeze.generate_session(api_secret=secret_key, session_token=session_token)  # Generating session using API key and authentication code

def get_spot_historical_data(start, end):  # Function to get historical spot data
    start = start.isoformat()[:19] + '.000Z'
    end = end.isoformat()[:19] + '.000Z'
    data = breeze.get_historical_data(interval="1day", from_date=start, to_date=end, stock_code=index_name, exchange_code="NSE", product_type="cash")
    df = pd.DataFrame(data['Success'])
    print(df)
    return df

def get_spot_daily_and_5min_data(start,end):  # Function to get spot daily and 5 minute data
    spot={}
    h=breeze.get_historical_data(interval="1day",
                            from_date= start.isoformat()[:19] + '.000Z',
                            to_date= end.isoformat()[:19] + '.000Z',
                            stock_code=index_name,
                            exchange_code="NSE",
                            product_type="cash")
    underlying_df_daily=pd.DataFrame(h['Success'])
    logging.info(underlying_df_daily)
    spot['daily'+end.strftime('%Y%m%d')]=underlying_df_daily[['datetime','open','high','low','close','volume']]
    
    h=breeze.get_historical_data(interval="5minute",
                            from_date= start.isoformat()[:19] + '.000Z',
                            to_date= end.isoformat()[:19] + '.000Z',
                            stock_code=index_name,
                            exchange_code="NSE",
                            product_type="cash")
    underlying_df_5min=pd.DataFrame(h['Success'])
    logging.info(underlying_df_5min) 
    spot['min5'+end.strftime('%Y%m%d')]=underlying_df_5min[['datetime','open','high','low','close','volume']]

    h=breeze.get_historical_data(interval="1minute",
                            from_date= start.isoformat()[:19] + '.000Z',
                            to_date= end.isoformat()[:19] + '.000Z',
                            stock_code=index_name,
                            exchange_code="NSE",
                            product_type="cash")
    underlying_df_1min=pd.DataFrame(h['Success'])
    logging.info(underlying_df_1min) 
    spot['min1'+end.strftime('%Y%m%d')]=underlying_df_1min[['datetime','open','high','low','close','volume']]

    return spot


def get_option_data_for_7days(expiry1, strike='42500', r='call'):  # Function to get option data for 7 days
    expiry=expiry1
    expiry = expiry.strftime('%Y-%m-%dT00:00:00.000Z')
    date1 = expiry1
    start_date = date1 - timedelta(days=past_days)
    end_date = date1 + timedelta(days=1)
    print(expiry1,start_date,end_date,strike,r)
    # Initialize an empty DataFrame to store all historical data
    all_data = pd.DataFrame()

    # Loop through 2-day intervals
    current_start = start_date
    while current_start < end_date:
        current_end = current_start + timedelta(days=2)
        if current_end > end_date:
            current_end = end_date

        # Format dates for the API
        start_str = current_start.strftime('%Y-%m-%dT00:00:00.000Z')
        end_str = current_end.strftime('%Y-%m-%dT00:00:00.000Z')

        # Fetch historical data for the current 2-day interval
        data = breeze.get_historical_data_v2(
            interval="1minute",
            from_date=start_str,
            to_date=end_str,
            stock_code=index_name,
            exchange_code="NFO",
            product_type="options",
            expiry_date=expiry,
            right=r,
            strike_price=strike
        )

        # Append the data to the main DataFrame
        if 'Success' in data:
            df = pd.DataFrame(data['Success'])
            all_data = pd.concat([all_data, df], ignore_index=True)

        # Move to the next interval
        current_start = current_end

    # Print the combined DataFrame
    # print(all_data)
    tt.sleep(0.2)
    return all_data

def get_strike_list(spot_data):  # Function to get a list of strike prices
    print(spot_data.columns)
    high = spot_data['high'].max()
    high = (int(float(high)) // strike_difference) * strike_difference + otm_buffer  # Convert float string to nearest 100 multiple int
    low = spot_data['low'].min()
    low = (int(float(low)) // strike_difference) * strike_difference - otm_buffer
    strike_list = []
    for i in range(int(low), int(high), strike_difference):
        strike_list.append(i)
    print(strike_list)
    return strike_list

def generate_expired_option_data(start, end):  # Function to generate expired option data
    print('starting')
    start1 = start
    year = start.year


    all_option_price_df = {}
    for month in range(start.month, end.month + 1):  
        all_month_expirys = [x for x in all_expirys if (x.month == month and x.year == year)]
        print(all_month_expirys)
        data=get_spot_daily_and_5min_data(start1,all_month_expirys[-1])
        all_option_price_df.update(data)
        print(all_month_expirys)
        for expiry in all_month_expirys:
            print("***************", start1, expiry)
            spot_data = get_spot_historical_data(start1, expiry)
            strike_list = get_strike_list(spot_data)
            for strike in strike_list:
                call_df = get_option_data_for_7days(expiry, strike, 'call')
                if not call_df.empty:
                    all_option_price_df['call' + str(strike) + expiry.strftime('%Y%m%d')] = call_df
                else:
                    logging.info('error in call' + str(strike) + expiry.strftime('%Y%m%d'))
                put_df = get_option_data_for_7days(expiry, strike, 'put')
                if not put_df.empty:
                    all_option_price_df['put' + str(strike) + expiry.strftime('%Y%m%d')] = put_df
                else:
                    logging.info('error in put' + str(strike) + expiry.strftime('%Y%m%d'))
            start1 = expiry
        start1 = all_month_expirys[-1]
        
    return all_option_price_df


all_option_price_df = generate_expired_option_data(start,end)  # Generating expired option data
# print(all_option_price_df)
store_into_database(all_option_price_df)  # Storing data into the database


print(start_time_before)
print(datetime.now())
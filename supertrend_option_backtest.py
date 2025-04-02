
# Bank Nifty Options Selling Strategy using Supertrend (10,3)
# Trading Rules:
# Indicator & Setup:
# We will use the Supertrend (10,3) indicator for both Bank Nifty and the option contracts.
# The strategy will begin monitoring the market from 9:30 AM.

# Entry Criteria:
# At 9:30 AM, check the Bank Nifty price relative to the Supertrend level:
# If Bank Nifty > Supertrend level → Sell a Put Option with a strike price closest to the Supertrend level.
# If Bank Nifty < Supertrend level → Sell a Call Option with a strike price closest to the Supertrend level.

# Stop Loss & Exit:

# Initial Stop Loss:
# The initial stop loss will be set at the Supertrend value of the sold option.

# Trailing Stop Loss:
# The stop loss will be trailing, using the Supertrend (10,3) of the option contract itself.
# If the option price rises above its own Supertrend level, we will exit the trade.

# Re-Entry After Exit:
# After the first trade exits (either Call or Put), we will wait for a fresh signal in the opposite direction:

# If the first trade was a Put sell and it exited, we will look for a Call sell opportunity when Bank Nifty < Supertrend level.
# If the first trade was a Call sell and it exited, we will look for a Put sell opportunity when Bank Nifty > Supertrend level.

# Maximum Entries & Trade Management:
# A maximum of 3 trades will be allowed in a day.

# After the second trade also exits, we will wait for a final entry opportunity in the opposite direction.
# If the third trade also hits stop loss, no further trades will be taken for the day.

#final exit:
# At 3:15 PM, all open positions will be exited regardless of their status.



# https://drive.google.com/file/d/1VG7opuN5JcZnCFFTE7_jyoivoZgOLcJK/view?usp=sharing



from datetime import datetime, timedelta  # Importing necessary modules
from calendar import monthrange, weekday, WEDNESDAY, THURSDAY
import pandas as pd
import numpy as np
import time as t
import sqlite3
import logging
import matplotlib.pyplot as plt

pd.options.mode.chained_assignment = None  # default='warn'

logging.basicConfig(filename='option_backtesting.log', filemode='w', level=logging.INFO, format=' %(message)s')  # Configuring logging
logging.info('this is my first line')  # Logging the first line

data_address='new_bank_nifty_jan_june.db'


# expiry_address='banknifty_expiry.csv'
# all_expirys=pd.read_csv(expiry_address)
# print(all_expirys['Expiry Date'].to_list())
# all_expirys = pd.to_datetime(all_expirys['Expiry Date']).to_list()
# print(all_expirys)

all_expiry=['2024-01-03', '2024-01-10', '2024-01-17', '2024-01-25', '2024-01-31', '2024-02-07', '2024-02-14', '2024-02-21', '2024-02-29', '2024-03-06', '2024-03-13', '2024-03-20', '2024-03-27', '2024-04-03', '2024-04-10', '2024-04-16', '2024-04-24', '2024-04-30', '2024-05-08', '2024-05-15', '2024-05-22', '2024-05-29', '2024-06-05', '2024-06-12', '2024-06-19', '2024-06-26', '2024-06-27', '2024-07-03', '2024-07-10', '2024-07-16', '2024-07-24', '2024-07-31', '2024-08-07', '2024-08-14', '2024-08-21', '2024-08-28', '2024-09-04', '2024-09-11', '2024-09-18', '2024-09-25', '2024-09-26', '2024-10-01', '2024-10-09', '2024-10-16', '2024-10-23', '2024-10-30', '2024-11-06', '2024-11-13', '2024-11-27', '2024-12-24', '2024-12-26', '2025-01-29', '2025-01-30', '2025-02-26', '2025-02-27', '2025-03-26', '2025-03-27']
all_expirys = [pd.to_datetime(x) for x in all_expiry]


def get_weekly_expiry(year, month):  # Function to get all Thursdays of the month
    all_month_expirys = [x for x in all_expirys if (x.month == month and x.year == year)]
    return all_month_expirys





def get_nearest_expiry(current_day=datetime.now()):
    # Get the current year and month based on the input 'current_day'
    year = current_day.year
    month = current_day.month

    all_exp = [x for x in all_expirys if (x.month == month and x.year == year)]

    # Find the current expiry date
    current_expiry = None
    for curr_exp in all_exp:
        if current_day <= curr_exp:
            current_expiry = curr_exp
            break

    # Handle the case when today is after the last Thursday of the month
    if current_day > all_exp[-1]:
        if month == 12:
            # Move to the next year and set month to January
            year += 1
            month = 1
        else:
            month += 1


        all_exp = [x for x in all_expirys if (x.month == month and x.year == year)]

        current_expiry = all_exp[0]
        return current_expiry

    return current_expiry

print('starting')


def get_from_database():
    con = sqlite3.connect(data_address)
    cursorObj = con.cursor()
    cursorObj.execute('SELECT name from sqlite_master where type= "table"')
    data= cursorObj.fetchall()
    option_price_df={}
    temp=0
    for i in data:
        # temp=temp+1
        # if temp==5:
        #     break
        k=i[0]
        option_price_df[k]=pd.read_sql_query(f'SELECT * FROM {k}',con)
        # print(option_price_df)
    return option_price_df


year=2024
month=1
money=2000
trades=open('trades.csv','w')
trades.write('time'+","+'option_contract_name' +","+'position'+','+'option_price'+','+'underlying_price'+','+"stop_price"+','+'balance'+'\n')


# start1 = datetime(year, month, 1)
option_price_df1=get_from_database()
option_price_df={}
# print(option_price_df1)
# #resample 1min to 5min and store in option_price_df

def supertrend(high, low, close, length=7, multiplier=3):
    """
    Supertrend function that matches pandas_ta.supertrend output.
    
    Args:
        high (pd.Series): Series of high prices
        low (pd.Series): Series of low prices
        close (pd.Series): Series of close prices
        length (int): The ATR period. Default: 7
        multiplier (float): The ATR multiplier. Default: 3.0
    
    Returns:
        pd.DataFrame: DataFrame with columns:
            SUPERT - The trend value
            SUPERTd - The direction (1 for long, -1 for short)
            SUPERTl - The long values
            SUPERTs - The short values
    """
    # Calculate ATR using the pandas_ta method (RMA - Rolling Moving Average)
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = true_range.ewm(alpha=1/length, adjust=False).mean()

    # Calculate basic bands
    hl2 = (high + low) / 2
    upperband = hl2 + (multiplier * atr)
    lowerband = hl2 - (multiplier * atr)

    # Initialize direction and trend
    direction = [1]  # Start with long
    trend = [lowerband.iloc[0]]  # Start with lowerband
    long = [lowerband.iloc[0]]
    short = [np.nan]

    # Iterate through the data to calculate the Supertrend
    for i in range(1, len(close)):
        if close.iloc[i] > upperband.iloc[i - 1]:
            direction.append(1)
        elif close.iloc[i] < lowerband.iloc[i - 1]:
            direction.append(-1)
        else:
            direction.append(direction[i - 1])
            if direction[i] == 1 and lowerband.iloc[i] < lowerband.iloc[i - 1]:
                lowerband.iloc[i] = lowerband.iloc[i - 1]
            if direction[i] == -1 and upperband.iloc[i] > upperband.iloc[i - 1]:
                upperband.iloc[i] = upperband.iloc[i - 1]

        if direction[i] == 1:
            trend.append(lowerband.iloc[i])
            long.append(lowerband.iloc[i])
            short.append(np.nan)
        else:
            trend.append(upperband.iloc[i])
            long.append(np.nan)
            short.append(upperband.iloc[i])

    # Create DataFrame to return
    df = pd.DataFrame({
        "SUPERT": trend,
        "SUPERTd": direction,
        "SUPERTl": long,
        "SUPERTs": short,
    }, index=close.index)

    return df['SUPERT']


for i,j in option_price_df1.items():
    if j.empty == False:
        j=j[['datetime','open','high','low','close','volume']]
        j['datetime']=pd.to_datetime(j['datetime'])
        j.set_index('datetime',inplace=True)
        ohlcv_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }
        option_price_df[i]=j.resample('5min').agg(ohlcv_dict)
        #drop nan rows
        option_price_df[i].dropna(inplace=True)
        #remove date before 9:15 and after 15:30
        option_price_df[i]=option_price_df[i].between_time('09:15','15:30')
        option_price_df[i].reset_index(inplace=True)
        data=option_price_df[i]
        #convert string column to int
        data['open']=data['open'].astype(float)
        data['high']=data['high'].astype(float)
        data['low']=data['low'].astype(float)
        data['close']=data['close'].astype(float)
        data['super']=supertrend(data['high'], data['low'], data['close'], length=10, multiplier=3)
        # data.dropna(inplace=True)
        option_price_df[i]=data




for month in range(1,7):
    end=get_weekly_expiry(year,month)[-1]
    start=datetime(year,month,1)
    underlying_df_daily=option_price_df['daily'+end.strftime('%Y%m%d')]
    underlying_df_5min=option_price_df['min5'+end.strftime('%Y%m%d')]

    
    for i in underlying_df_daily.index:
        open_price=underlying_df_daily['open'][i]
        time=underlying_df_daily['datetime'][i]
        if time>datetime(2024,6,27):
            break
        #skip fo this days no_data=['2024-01-20','2024-03-02','2024-05-09','2024-05-18','2024-06-26']
        if time.strftime('%Y-%m-%d') in ['2024-01-20','2024-03-02','2024-05-09','2024-05-18','2024-06-26']:
            continue
        start=datetime(time.year,time.month,time.day,9,20)
        end2=datetime(time.year,time.month,time.day,15,15)
        portfolio={}
        first_trade=False
        stop_loss=None
        max_entries = 3
        entries = 0
        position = None
        stop_loss = None

     
        while start<=end2:
            try:
                current_expiry=get_nearest_expiry(time).strftime('%Y%m%d')
                current_time=start.strftime('%Y-%m-%d %H:%M:%S')
                spot_price=underlying_df_5min[underlying_df_5min['datetime']==current_time].close.values[0]
                spot_super=underlying_df_5min[underlying_df_5min['datetime']==current_time].super.values[0]

                atm1=round(spot_price / 100) * 100
                
                atm_call='call'+ str(atm1)+ current_expiry
                atm_put='put'+ str(atm1)+ current_expiry

                atm_call_price=option_price_df[atm_call][option_price_df[atm_call]['datetime']==current_time].close.values[0]
                atm_put_price=option_price_df[atm_put][option_price_df[atm_put]['datetime']==current_time].close.values[0]
                # print(current_time,spot_price,supertrend,atm_call,atm_call_price,atm_put,atm_put_price)
                if (first_trade==False)  and  (start.time() == datetime(time.year,time.month,time.day,9,30).time()):
                    # print(spot_price,spot_super)
                    if spot_price>spot_super:
                        entries=entries+1
                        # print(current_time,'sell put')
                        first_trade=True
                        strike=str(round(spot_super/100)*100)
                        name='put'+strike+current_expiry
                        # print(name)

                        put_price=option_price_df[name][option_price_df[name]['datetime']==current_time].close.values[0]
                        # print(put_price)
                        money=money+put_price

                        
                        stop_price=option_price_df[name][option_price_df[name]['datetime']==current_time].super.values[0]
                        stop_difference=abs(put_price-stop_price)

                        position={'type': 'put', 'strike': strike, 'entry_price': put_price, 'stop_price': stop_price, 'stop_difference': stop_difference,'name': name}
                        logging.info(f"{current_time} selling put: {name}, price: {put_price}, stop loss: {stop_price}")
                        trades.write(start.strftime('%Y-%m-%d %H:%M:%S')+" , "+str(name)+","+'sell'+','+str(put_price)+','+str(spot_price)+","+str(stop_price)+','+str(money)+'\n')
                       

                    elif spot_price<spot_super:
                        entries=entries+1
                        # print(current_time,'sell call')
                        first_trade=True
                        strike=str(round(spot_super/100)*100)
                        name='call'+strike+current_expiry
                        # print(name)

                        call_price=option_price_df[name][option_price_df[name]['datetime']==current_time].close.values[0]
                        # print(call_price)
                        money=money+call_price

                        
                        stop_price=option_price_df[name][option_price_df[name]['datetime']==current_time].super.values[0]
                        stop_difference=abs(call_price-stop_price)

                        position={'type': 'call', 'strike': strike, 'entry_price': call_price, 'stop_price': stop_price, 'stop_difference': stop_difference,'name': name}
                        logging.info(f"{current_time} selling call: {name}, price: {call_price}, stop loss: {stop_price}")
                        trades.write(start.strftime('%Y-%m-%d %H:%M:%S')+" , "+str(name)+","+'sell'+','+str(call_price)+','+str(spot_price)+","+str(stop_price)+','+str(money)+'\n')

                elif (position is not None) and (start.time() == datetime(time.year,time.month,time.day,15,15).time()):
                    # print(current_time,'exiting position')
                    name=position['name']
                    current_price=option_price_df[name][option_price_df[name]['datetime']==current_time].close.values[0]
                    stop_price=position['stop_price']
                    super=option_price_df[name][option_price_df[name]['datetime']==current_time].super.values[0]
                    money=money-current_price
                    position=None
                    # money=money-stop_difference
                    logging.info(f"{current_time} exiting position: , {name}, price: {current_price}")
                    trades.write(start.strftime('%Y-%m-%d %H:%M:%S')+" , "+str(name)+","+'exit'+','+str(current_price)+','+str(spot_price)+","+str(stop_loss)+','+str(money)+'\n')

                elif position is not None:
               
                    name=position['name']
                    current_price=option_price_df[name][option_price_df[name]['datetime']==current_time].close.values[0]
                    stop_price=position['stop_price']
                  
                    super=option_price_df[name][option_price_df[name]['datetime']==current_time].super.values[0]
                    if (current_price>stop_price):
                        # print(current_time,'exiting position')
                        position=None
                        money=money-current_price
                        # money=money-stop_difference
                        logging.info(f"{current_time} exiting position: {name}, price: {current_price}")
                        trades.write(start.strftime('%Y-%m-%d %H:%M:%S')+" , "+str(name)+","+'exit'+','+str(current_price)+','+str(spot_price)+","+str(stop_loss)+','+str(money)+'\n')
                    elif (current_price<stop_price):
                        #trail stop loss
                        position['stop_price']=super
                        # logging.info(f"{current_time} trailing stop loss: {name}, price: {current_price}, stop loss: {super}")
              
                elif (position is None) and (first_trade is True) and (entries<max_entries):

                    # print(spot_price,spot_super)
                    if spot_price>spot_super:
                        entries=entries+1
                        # print(current_time,'sell put')
                        
                        strike=str(round(spot_super/100)*100)
                        name='put'+strike+current_expiry
                        # print(name)

                        put_price=option_price_df[name][option_price_df[name]['datetime']==current_time].close.values[0]
                        # print(put_price)
                        money=money+put_price

                        
                        stop_price=option_price_df[name][option_price_df[name]['datetime']==current_time].super.values[0]
                        stop_difference=abs(put_price-stop_price)

                        position={'type': 'put', 'strike': strike, 'entry_price': put_price, 'stop_price': stop_price, 'stop_difference': stop_difference,'name': name}
                        logging.info(f"{current_time} selling put: {name}, price: {put_price}, stop loss: {stop_price}")
                        trades.write(start.strftime('%Y-%m-%d %H:%M:%S')+" , "+str(name)+","+'sell'+','+str(put_price)+','+str(spot_price)+","+str(stop_price)+','+str(money)+'\n')
                       

                    elif spot_price<spot_super:
                        entries=entries+1
                        # print(current_time,'sell call')
                        
                        strike=str(round(spot_super/100)*100)
                        name='call'+strike+current_expiry
                        # print(name)

                        call_price=option_price_df[name][option_price_df[name]['datetime']==current_time].close.values[0]
                        # print(call_price)
                        money=money+call_price

                        
                        stop_price=option_price_df[name][option_price_df[name]['datetime']==current_time].super.values[0]
                        stop_difference=abs(call_price-stop_price)

                        position={'type': 'call', 'strike': strike, 'entry_price': call_price, 'stop_price': stop_price, 'stop_difference': stop_difference,'name': name}
                        logging.info(f"{current_time} selling call: {name}, price: {call_price}, stop loss: {stop_price}")
                        trades.write(start.strftime('%Y-%m-%d %H:%M:%S')+" , "+str(name)+","+'sell'+','+str(call_price)+','+str(spot_price)+","+str(stop_price)+','+str(money)+'\n')



                start=start+timedelta(minutes=5)

            
            except Exception as e:
                print(start,current_expiry,spot_price,supertrend,atm_call,atm_put,atm_call_price,atm_put_price)
                logging.info('eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'+str(atm_call)+str(atm_put)+','+str(start)+','+str(end))
                # print('eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'+str(atm)+','+str(start)+','+str(end))
                start=start+timedelta(days=1)
                print(current_time,e)
              
                # print(underlying_df_5min)
                break
        

print(money)
print('final money is ',str(money))
logging.info('final money is: '+str(money))
trades.close()




# Load the data
df = pd.read_csv('trades.csv')
print(df.shape)
# Clean and prepare the data
df['time'] = pd.to_datetime(df['time'])
df['date'] = df['time'].dt.date
df['pnl'] = np.where(df['position'] == 'sell', df['balance'].diff(), 0)
df['pnl'] = np.where(df['position'] == 'exit', df['balance'] - df['balance'].shift(1), df['pnl'])

# Extract trade pairs (sell + exit)
trades = []
current_trade = None

for _, row in df.iterrows():
    if row['position'] == 'sell':
        if current_trade is not None:
            # Close the previous trade if it wasn't closed properly
            trades.append(current_trade)
        current_trade = {
            'entry_time': row['time'],
            'exit_time': None,
            'contract': row['option_contract_name'],
            'entry_price': row['option_price'],
            'exit_price': None,
            'underlying_entry': row['underlying_price'],
            'underlying_exit': None,
            'pnl': None,
            'duration': None,
            'stop_price': row['stop_price']
        }
    elif row['position'] == 'exit' and current_trade is not None:
        current_trade['exit_time'] = row['time']
        current_trade['exit_price'] = row['option_price']
        current_trade['underlying_exit'] = row['underlying_price']
        current_trade['pnl'] = current_trade['entry_price'] - current_trade['exit_price']
        current_trade['duration'] = (current_trade['exit_time'] - current_trade['entry_time']).total_seconds() / 60
        trades.append(current_trade)
        current_trade = None

# Convert trades to DataFrame
trades_df = pd.DataFrame(trades)
print(trades_df.to_csv('all_trades.csv', index=False))

# Calculate metrics
total_trades = len(trades_df)
winning_trades = len(trades_df[trades_df['pnl'] > 0])
losing_trades = len(trades_df[trades_df['pnl'] < 0])
win_rate = winning_trades / total_trades * 100
avg_win = trades_df[trades_df['pnl'] > 0]['pnl'].mean()
avg_loss = trades_df[trades_df['pnl'] < 0]['pnl'].abs().mean()
profit_factor = (winning_trades * avg_win) / (losing_trades * avg_loss) if losing_trades > 0 else np.inf
total_pnl = trades_df['pnl'].sum()
avg_pnl_per_trade = total_pnl / total_trades
max_win = trades_df['pnl'].max()
max_loss = trades_df['pnl'].min()
avg_trade_duration = trades_df['duration'].mean()

# Calculate equity curve
equity_curve = df[df['position'] == 'exit'][['time', 'balance']].copy()
equity_curve.set_index('time', inplace=True)

# Calculate drawdown
equity_curve['peak'] = equity_curve['balance'].cummax()
equity_curve['drawdown'] = (equity_curve['balance'] - equity_curve['peak']) / equity_curve['peak'] * 100
max_drawdown = equity_curve['drawdown'].min()

# Calculate daily performance
daily_pnl = df[df['position'] == 'exit'].groupby('date')['balance'].last().diff().dropna()
avg_daily_pnl = daily_pnl.mean()
winning_days = len(daily_pnl[daily_pnl > 0])
losing_days = len(daily_pnl[daily_pnl < 0])
daily_win_rate = winning_days / (winning_days + losing_days) * 100

# Print performance metrics
print("=== Performance Metrics ===")
print(f"Total Trades: {total_trades}")
print(f"Winning Trades: {winning_trades} ({win_rate:.2f}%)")
print(f"Losing Trades: {losing_trades} ({100 - win_rate:.2f}%)")
print(f"Average Win: {avg_win:.2f}")
print(f"Average Loss: {avg_loss:.2f}")
print(f"Profit Factor: {profit_factor:.2f}")
print(f"Total P&L: {total_pnl:.2f}")
print(f"Average P&L per Trade: {avg_pnl_per_trade:.2f}")
print(f"Max Win: {max_win:.2f}")
print(f"Max Loss: {max_loss:.2f}")
print(f"Average Trade Duration (minutes): {avg_trade_duration:.2f}")
print(f"Max Drawdown: {max_drawdown:.2f}%")
print(f"\nDaily Performance:")
print(f"Average Daily P&L: {avg_daily_pnl:.2f}")
print(f"Winning Days: {winning_days} ({daily_win_rate:.2f}%)")
print(f"Losing Days: {losing_days}")

# Plot equity curve
plt.figure(figsize=(12, 6))
plt.plot(equity_curve.index, equity_curve['balance'], label='Equity Curve')
plt.title('Equity Curve')
plt.xlabel('Date')
plt.ylabel('Balance')
plt.grid(True)
plt.legend()
plt.show()

# Plot drawdown
plt.figure(figsize=(12, 6))
plt.plot(equity_curve.index, equity_curve['drawdown'], label='Drawdown', color='red')
plt.title('Drawdown')
plt.xlabel('Date')
plt.ylabel('Drawdown (%)')
plt.grid(True)
plt.legend()
plt.show()

# Plot P&L distribution
plt.figure(figsize=(12, 6))
plt.hist(trades_df['pnl'], bins=30, edgecolor='black')
plt.title('P&L Distribution')
plt.xlabel('P&L')
plt.ylabel('Frequency')
plt.grid(True)
plt.show()

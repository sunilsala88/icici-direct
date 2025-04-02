import credentials as cs
api_key = cs.api_key
secret_key = cs.secret_key
with open('access.txt', 'r') as file:
    session_token = file.read().strip()


from breeze_connect import BreezeConnect  # Importing BreezeConnect module
breeze = BreezeConnect(api_key=api_key)  # Initializing BreezeConnect SDK

breeze.generate_session(api_secret=secret_key, session_token=session_token)  # Generating session using API key and authentication code


# data=breeze.get_historical_data(interval="5minute",
#                   from_date= "2025-02-03T09:21:00.000Z",
#                   to_date= "2025-02-04T09:21:00.000Z",
#                   stock_code="NIFTY",
#                   exchange_code="NFO",
#                   product_type="futures",
#                   expiry_date="2025-02-27T07:00:00.000Z",
#                   right="others",
#                   strike_price="0")  
# # print(data)
import pandas as pd
# df=pd.DataFrame(data['Success'])
# print(df)


# data1=breeze.get_historical_data(interval="5minute",
#                   from_date= "2025-02-03T09:20:00.000Z",
#                   to_date= "2025-02-04T09:22:00.000Z",
#                   stock_code="NIFTY",
#                   exchange_code="NFO",
#                   product_type="options",
#                   expiry_date="2025-02-06T07:00:00.000Z",
#                   right="call",
#                   strike_price="23200")

# print(data1)
# df1=pd.DataFrame(data1['Success'])
# print(df1.to_csv('sample.csv'))


data3=breeze.get_historical_data_v2(interval="1minute",
                    from_date= "2025-02-03T09:20:00.000Z",
                    to_date= "2025-02-07T09:21:00.000Z",
                    stock_code="NIFTY",
                    exchange_code="NFO",
                    product_type="options",
                    expiry_date="2025-02-06T07:00:00.000Z",
                    right="call",
                    strike_price="23200")

df3=pd.DataFrame(data3['Success'])
print(df3)
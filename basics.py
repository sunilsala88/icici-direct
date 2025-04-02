from breeze_connect import BreezeConnect
import os
import certifi
os.environ['SSL_CERT_FILE'] = certifi.where()

import credentials as cs
api_key = cs.api_key
secret_key = cs.secret_key
with open('access.txt', 'r') as file:
    session_token = file.read().strip()

# Initialize SDK
breeze = BreezeConnect(api_key=api_key)


# Generate Session
breeze.generate_session(api_secret=secret_key,
                        session_token=session_token)

# # Connect to websocket(it will connect to tick-by-tick data server)
# breeze.ws_connect()

data=breeze.get_historical_data(interval="1day",
                  from_date= "2025-02-03T09:20:00.000Z",
                  to_date= "2025-02-05T09:22:00.000Z",
                  stock_code="RELIND",
                  exchange_code="NSE",
                  product_type="cash")
print(data)
import pandas as pd
df=pd.DataFrame(data['Success'])
print(df)





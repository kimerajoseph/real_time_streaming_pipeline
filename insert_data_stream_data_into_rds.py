import base64
import json
from datetime import datetime
import ast
import os

import pymysql
import pandas as pd
from sqlalchemy import create_engine

print('Loading function')

my_list, final_list = [],[]
def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        # convert json to dictionary and create list
        new_data = ast.literal_eval(str(payload))
        my_list.append(new_data)
        
    # process data
    # change date from ISO8601 formart
    for item in my_list:
        date = datetime.fromisoformat(item['event_time']).date()
        time = datetime.fromisoformat(item['event_time']).time().replace(microsecond=0)
        final_list.append([date,time,item['ticker'],item['price']])
    
    host_name = os.environ['host_name']  
    db_name = os.environ['db_name'] 
    user = os.environ['user'] 
    password = os.environ['password'] 
    
    print("FINAL DATA ", my_list)
    conn = pymysql.connect(
    host=host_name,
    port=3306,
    user=user,
    password=password,
    charset='utf8mb4')
    
    connection_string = 'mysql+pymysql://' + user + ':' + password + '@'+ host_name + ':3306/'+ db_name
    engine = create_engine(connection_string)
    
    df = pd.DataFrame(final_list)
    df.columns = ['date','time','ticker','price']
    #df.head()
    
    #Insert data into MySQL DB. 
    df.to_sql('stream_stock_data', con=engine, if_exists='append', index=False)
    print("PROCESSING DONE")
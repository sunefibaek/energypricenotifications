# Databricks notebook source
from datetime import datetime, timedelta
import requests, json
import pandas as pd
today = datetime.now().date()# - timedelta(days=1)
tomorrow = datetime.now().date() + timedelta(days=1)
format = "%H:%M"
start_time = datetime.now().strftime(format)
end_time = (datetime.now() + timedelta(hours=25)).strftime(format)
query_str = f"https://api.energidataservice.dk/dataset/Elspotprices?offset=0&start={today}T{start_time}&end={tomorrow}T{end_time}&filter=%7B%22PriceArea%22:[%22DK2%22,%22DK1%22]%7D&sort=HourUTC%20DESC&timezone=dk"
print(query_str)

# COMMAND ----------

r = requests.get(query_str)
data = r.json()
entries = data["records"]


# COMMAND ----------

df = pd.DataFrame(entries).sort_values(by = ['SpotPriceDKK'])
df_data = df[['HourDK','SpotPriceDKK','PriceArea']]
dict_data = df_data.to_dict()
print(dict_data['SpotPriceDKK'])

# COMMAND ----------

rslt_df = df_data[(df_data['PriceArea'] == 'DK2')] 
#>>> df.loc[df['Host'] == 'a', 'Port'].item()
text_str = rslt_df.loc[(df_data['PriceArea'] == 'DK2')].values[0]
type(text_str)
#print(text_str)
#print(df_data['SpotPriceDKK']/1000)

# COMMAND ----------

requests.post("https://ntfy.sh/BeskedOmPrisenPaaStrom", data=f'{text_str}'.encode(encoding='utf-8'))

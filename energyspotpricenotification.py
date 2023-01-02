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

# Get spotprices for DK1 and DK2 based on the query string above. 

prices_request = requests.get(query_str)
prices_data = prices_request.json()
prices_entries = prices_data["records"]

# COMMAND ----------

rates_request = requests.get("https://openexchangerates.org/api/latest.json?app_id=3670852a85514451bf0165755fabda52&prettyprint=false&show_alternative=false")
rates_data = rates_request.json()
rates_entries = rates_data["rates"]

# COMMAND ----------

# Currency conversion (from USD to EUR to DKK because the free sccount on openexchange does not allow to change the base)
# Not pretty and a bit crude, but it is a decent approximation none the less. 
# Conversion needed as the DKK price is not updated on weekends and public holidays

def currency_conversion(price_eur):
# price_in_eur/eur_to_usd_rate*dkk_to_usd_rate
  value_in_dkk = round(((price_eur/rates_entries["EUR"])*rates_entries["DKK"])/1000,2)
  print(value_in_dkk)

# COMMAND ----------

df = pd.DataFrame(prices_entries).sort_values(by = ['SpotPriceDKK'])
df_data = df[['HourDK','SpotPriceDKK','SpotPriceEUR','PriceArea']]
dict_data = df_data.to_dict()
print(dict_data["SpotPriceEUR"])

# COMMAND ----------



# COMMAND ----------

def post_update(PriceArea):
  rslt_df = df_data[(df_data['PriceArea'] == PriceArea)] 
  text_str = rslt_df.loc[(df_data['PriceArea'] == PriceArea)].values[0]
  requests.post("https://ntfy.sh/BeskedOmPrisenPaaStrom", data=f'{text_str}'.encode(encoding='utf-8'))
  print(text_str)

# COMMAND ----------

post_update('DK2')

# COMMAND ----------

rslt_df = df_data[(df_data['PriceArea'] == 'DK2')] 
#>>> df.loc[df['Host'] == 'a', 'Port'].item()
text_str = rslt_df.loc[(df_data['PriceArea'] == 'DK2')].values[0]
type(text_str)
#print(text_str)
#print(df_data['SpotPriceDKK']/1000)

# COMMAND ----------

requests.post("https://ntfy.sh/BeskedOmPrisenPaaStrom", data=f'{text_str}'.encode(encoding='utf-8'))

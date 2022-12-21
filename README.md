# energypricenotifications
Small python script to retrieve energy spot price data from the Danish energy data service API: https://www.energidataservice.dk/tso-electricity/elspotprices
The script uses the ntfy (https://github.com/dschep/ntfy) app to send a notification to a users cell phone with the lowest price within the next 24 hours. 

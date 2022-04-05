from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from urllib.parse import urlencode
from bs4 import BeautifulSoup
from datetime import date
import pandas as pd
import numpy as np
import requests
import re

def req_retry_session(
    retries = 3,
    backoff_factor = 0.3,
    status_forcelist = (500, 502, 504),
    session = None,
):
	'''Returns request session with multiple retries
	
	Parameters:
		retries (int): Number of retries, default 3
		backoff_factor (int): Backoff factor, default 0.3
		status_forcelist (tuple): Tuple of status codes to force, default (500, 502, 504)
		session: Session
	Returns:
		Request session with multiple retries
    '''
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def adjustPrice(ex_date = pd.to_datetime('today'), currency = 'EUR'):
	'''Returns the exchanged and inflation adjusted unit (100) rate
	
	Parameters:
		ex_date (datetime): A datetime representing the exchange date, default today
		currency (str): Currency code (ISO 4217) (i.e.: EUR, USD), default EUR
		
	Returns:
		Exchanged and inflation adjusted unit (100) rate (float)
    '''
    # scrape exchanged inflation adjusted price from fxtop.com
    prices = []
    month_str = str(ex_date.month)
    day_str = str(ex_date.day)
    if(month_str.isdigit() and 1 <= int(month_str) <= 9):
        month_str = '0'+ month_str
    if(day_str.isdigit() and 1 <= int(day_str) <= 9):
        day_str = '0'+ day_str
    url = 'https://fxtop.com/en/historical-currency-converter.php?A=100&C1='+currency+'&C2=EUR&DD='+day_str+'&MM='+month_str+'&YYYY='+str(ex_date.year)+'&B=1&P=&I=1&btnOK=Go%21'
    try:
        response = req_retry_session().get(url)
        page = response.text
        soup = BeautifulSoup(page, "html.parser")
        
        # navigate all links to get the inflated price with the correct currency
        prices = soup.find_all('a', {'title': 'convert currencies at end date'})
        if(len(prices)==1): return float(prices[0].text.split('EUR')[0].replace(" ", ""))
        elif(currency=='EUR'):
            try:
                return float(prices[0].text.split(currency)[0].replace(" ", ""))
            except IndexError:
                return False
        else:
            try:
                return float(prices[1].text.split('EUR')[0].replace(" ", ""))
            except IndexError:
                return False
    except requests.exceptions.RequestException as e:
        return False

def rateConverter(df, date_col, curr_col):
	'''Returns the exchanged and inflation adjusted unit (100) rate
	
	Parameters:
		df (Pandas.DataFrame): A dataframe with currency and exchage date columns
		date_col (str): The datetime column name of the dataframe with exchange dates
		curr_col (str): The currencies column name of the dataframe with exchange currencies
		
	Returns:
		The list of exchange rates adjusted with date and inflation, if not found returns -1 (float)
    '''
    # create a dictionary of rates for all currencies for all dates from 2001
    rates = []
    
    # make a new data frame of all auction dates and the related currencies
    iter_frame = df[[date_col, curr_col]].copy()
    # set missing currencies to EUR
    iter_frame.loc[iter_frame[curr_col].isnull(), curr_col] = 'EUR'
    size = len(iter_frame[date_col])
    
    for i, (x, y) in enumerate(zip(iter_frame[date_col], iter_frame[curr_col])):
        if(adjustPrice(x, y)):
            rates.append(adjustPrice(x, y))
        else:
            rates.append(-1)
    return rates
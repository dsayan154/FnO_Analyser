import logging
from bs4 import BeautifulSoup
import pandas as pd, requests

logging.basicConfig(level=logging.DEBUG)
stockOptsUrl = 'https://www.moneycontrol.com/stocks/fno/marketstats/options/active_value/homebody.php?opttopic=active_value&optinst=stkopt&sel_mth=1&sort_order=0'
# commonHeaders = {'user-agent': 'fno_analyser'}
def getStockOptions() -> pd.DataFrame:
    '''
    scrapes stock options data from moneycontrol: https://www.moneycontrol.com/stocks/fno/marketstats/options/active_value/homebody.php?opttopic=active_value&optinst=stkopt&sel_mth=1&sort_order=0
    and returns a pandas dataframe
    '''
    logging.debug('inside getStockOptions in moneycontrol module')
    try:
        # headers = commonHeaders
        # response = requests.get(headers=headers, url=stockOptsUrl)
        # soup = BeautifulSoup(response.text)
        # table = soup.find('div', {'class':'MT15'}).findChild('table', attrs={'class': 'tblList'})
        stockOptionsDf = pd.read_html(stockOptsUrl, attrs={'class': 'tblList'})[0]
        logging.debug(f'dataframe generated: {stockOptionsDf}')
        return stockOptionsDf
    except Exception as e:
        logging.debug(f'error occurred in getOptions in moneycontrol module: {e}')
        pass
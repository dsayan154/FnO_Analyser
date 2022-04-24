import logging
from urllib import parse
from bs4 import BeautifulSoup
import pandas as pd, requests

logging.basicConfig(level=logging.DEBUG)
stockOptsPage = {
                    'url': 'https://www.moneycontrol.com/stocks/fno/marketstats/options/active_value/homebody.php?opttopic=active_value&optinst=stkopt&sel_mth=1&sort_order=0', 
                    'tbl_attr': {'class': 'tblList'}, 
                    'cols_to_split': {
                                        'High Low': ['High', 'Low'], 
                                        'Open Int Chg': ['OI Change','OI Change %']
                                    }
                }
stockFutPage = {
                    'url': 'https://www.moneycontrol.com/stocks/fno/marketstats/futures/most_active/homebody.php?opttopic=most_active&optinst=stkfut&sel_mth=1&sort_order=0', 
                    'tbl_attr': {'class': 'tblList'},
                    'cols_to_split': {
                                            'High Low': ['High', 'Low'], 
                                            'Open Int Chg': ['OI Change','OI Change %']
                                    }
                }
commonHeaders = {'user-agent': 'fno_analyser'}

def getDataFrame(instrument:str, expiryMonth:int=1, headers:dict=commonHeaders) -> pd.DataFrame:
    '''
    scrapes moneycontrol.com and returns a pandas dataframe containing either stock options chain or futures, depending on the 'instrument' parameter passed. The returned dataframe is sorted decreasingly by 'Value (Rs. Lakh)' column.
    ### Parameters
    1. instrument : str 
                financial instrument, accepted values: 'options', 'futures'
    2. expiryMonth : int
                expiry month index of the intrument, accepted values: 1(current month), 2(next month), 3(next to next month)
    3. headers : dict
                headers to be passed while fetching the page for moneycontrol.com
    '''

    logging.debug('inside getStockOptions in moneycontrol module')
    instrumentDetails: dict = {}
    if instrument == 'options':
        instrumentDetails = stockOptsPage
    elif instrument == 'futures':
        instrumentDetails = stockFutPage
    else:
        raise ValueError(f'invalid instrument passed: {instrument}, accepted values: futures, options')
    if 3 < expiryMonth or expiryMonth < 0:
        raise ValueError(f'expiryMonth should be either 1, 2, 3. got: {expiryMonth}')
    url=instrumentDetails['url']
    urlParts = parse.urlparse(url)
    qs = parse.parse_qsl(urlParts.query)
    qs[2] = ('sel_mth', str(expiryMonth))
    url = parse.urlunparse((urlParts.scheme, urlParts.netloc, urlParts.path, urlParts.params, parse.urlencode(qs), urlParts.fragment))
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise response.raise_for_status()
    df = pd.read_html(_getTable(response.text, instrumentDetails['tbl_attr']))[0]
    logging.debug(f'dataframe generated: {df}')
    logging.info(f'Splitting columns: {instrumentDetails["cols_to_split"]}')
    for col, newcols in instrumentDetails['cols_to_split'].items():
        df[newcols] = df[col].str.split(expand=True)
        df = df.drop(col, axis=1)
    return df

def _getTable(mcHtmlPage:str, attribs:dict) -> str:
    '''
    Get HTML code for table as extracted as per the HTML attribs passed, the function also replaces the br in mcHtmlPage table rows with a whitespace.
    '''
    logging.debug('inside _getTable')
    soup = BeautifulSoup(mcHtmlPage)
    table = soup.find('table', attrs=attribs)
    for tr in table.find_all('tr'):
        for br in tr.find_all('br'):
            br = br.replace_with(' ')
    return str(table)
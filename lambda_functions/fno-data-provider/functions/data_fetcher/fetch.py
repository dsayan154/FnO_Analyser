from datetime import datetime
import logging, os, boto3, requests, yaml, pandas as pd
from bs4 import BeautifulSoup
from copy import deepcopy

logLevel = os.getenv('LOG_LEVEL')
environment = os.getenv('ENVIRONMENT')
configParameterPrefix = os.getenv('CONFIG_PARAMETER_PREFIX')
configParameterName = os.getenv('CONFIG_PARAMETER_NAME')
nseHolidaysParameterName = os.getenv('NSE_HOLIDAY_PARAMETER_NAME')
config: dict = {}
logging.basicConfig(level=logging.DEBUG)

def lambda_handler(event, context):
    """Lambda handler to fetch records from moneycontrol stock options url and push the details to s3.

    This Lambda is meant to be sheduled.

    Parameters
    ----------
    event: dict, required
        Input event to the Lambda function

    context: object, required
        Lambda Context runtime methods and attributes

    Returns
    ------
        dict: returns dict containing stock option chains of all stocks from moneycontrol.
    """
    start()

def start():
    """
    Start processing. This function takes help of other functions to accomplish the following tasks:
    1. Get application config and nse holiday list from AWS SSM Parameter store.
    1. Fetch data from moneycontrol as pandas dataframe. (moneycontrol url and table index is mentioned from environment variables.)
    2. Fetch raw moneycontrol data in json format from s3 bucket. (s3 bucket name and prefix is mentioned in the environment variables.)
    3. Convert the dataframe fetched in 1. to json
    4. compare the json documents in step 2. and 3.
        a. If different then: goto 5.
        b. else: exit()
    5. Fetch the unique stock name from the dataframe -> list
    6. For each of the stock name from 5. fetch the opening price -> dict
    7. For each stock name in 5. retrieve 12 rows with strike prices greater than opening price and 12 rows with strike prices lesser than opening price. Append in a single dataframe.
    8. Convert the dataframe from 7. into json document and upload to s3
    9. return the dict 
    """
    global config
    ssm = boto3.client('ssm')
    logging.info('getting config and nse holiday list from SSM Parameter store')
    yamlConfig = ssm.get_parameter(Name=f'{configParameterPrefix}/{environment}/{configParameterName}')['Parameters']['Value']
    config = yaml.safe_load(yamlConfig)
    nseHolidayList = ssm.get_parameter(Name=f'{configParameterPrefix}/{environment}/{nseHolidaysParameterName}')['Parameters']['Value'].split(',')
    today = datetime.now().strftime('%d-%b-%Y')
    if today in nseHolidayList:
        logging.warn(f'today: {today} is NSE holiday. Exiting..')
        exit(0)   

def fetchMcOptsData() -> pd.DataFrame:
    logging.info(f'inside fetchMcOptsData to create filtered dataframe')
    mcOptsConfig = config['urlConfigs']['stocks']['options']
    mcOptsUrl = mcOptsConfig['url']
    mcOptsUrlHeaders = mcOptsConfig['url_headers']
    mcOptsTblAttr = mcOptsConfig['tbl_attr']
    mcOptsColsToSplit = mcOptsConfig['cols_to_split']
    rawDf = pd.read_html(_getTable(mcOptsUrl, mcOptsUrlHeaders, mcOptsTblAttr))[0]
    _splitRows(rawDf, mcOptsColsToSplit)
    stocks = list(rawDf['Symbol'].unique())
    for stock in stocks:
        stockDf = rawDf.loc[rawDf['Symbol'] == stock]
        stockDfCE = stockDf.loc[stockDf['Option Type'] == 'CE'].reset_index(drop = True).copy(deep=True)
        stockDfPE = stockDf.loc[stockDf['Option Type'] == 'PE'].reset_index(drop = True).copy(deep=True)
    
def _getTable(url: str, urlHeaders: dict, attrs: dict) -> str:
    """
    fetches a html code of the table with attribute attrs from the input url
    """
    logging.info(f'inside _getTable for {url}')
    response = requests.get(url, headers=urlHeaders)
    if response.status_code != 200:
        logging.warn(f'{url} returned unexpected status code: {response.status_code}')
    soup = BeautifulSoup(response.text)
    table = soup.find('table', attrs=attrs)
    for tr in table.find_all('tr'):
        for br in tr.find_all('br'):
            br = br.replace_with(' ')
    return str(table)

def _splitRows(df: pd.DataFrame, colsToSplit: dict) -> pd.DataFrame:
    """
    Inputs a DataFrame and splits the columns into two upon ' ' delimiter
    
    Parameters
    ----------
    df: pd.DataFrame, required
        DataFrame to operate on.

    colsToSplit: dict, required
        columns names to splits and the new column names. 
        Example: {'High Low': ['High', 'Low'], 'Open Int Chg': ['OI Change', 'OI Change %'], 'Vol - Shares Contracts': ['Vol', 'Shares Contracts'], 'Change Chg %': ['Change', 'pChange']}
    """
    logging.info('inside _splitRows')
    for colName, newCols  in colsToSplit:
        df[newCols] = df[colName].str.split(expand=True)
        df.drop(colName, axis='columns')
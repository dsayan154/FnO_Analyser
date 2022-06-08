from datetime import datetime, timedelta
import logging, os, boto3, requests, yaml, json, sqlalchemy, base64, pandas as pd
from bs4 import BeautifulSoup
from botocore.exceptions import ClientError

logLevel = os.getenv('LOG_LEVEL', 'INFO').upper()
environment = os.getenv('ENVIRONMENT', 'prod')
ssmParameterPrefix = os.getenv('SSM_PARAMETER_PREFIX', '/fno-stock-data-provider')
configParameterName = os.getenv('CONFIG_PARAMETER_NAME', 'data-fetcher-config')
nseHolidaysParameterName = os.getenv('NSE_HOLIDAY_PARAMETER_NAME', 'nse-holiday-list')
activityConditionsParameterName = os.getenv('ACTIVITY_CONDITIONS_PARAMETER_NAME', 'activity-conditions')
openingPriceFileName = os.getenv('OPENING_PRICE_FILE_NAME', 'opening_prices.json')
openingPriceBucketName = os.getenv('OPENING_PRICE_BUCKET_NAME', 'fnoanalyser')
dbCredentialsSecretName = os.getenv('DB_SECRET_NAME', 'test/fno_analyser/mariadb')
dbCredentialsSecretRegion = os.getenv('DB_SECRET_REGION', 'ap-south-1')
optionsChainTableName = os.getenv('OPTIONS_CHAIN_TABLE_NAME', 'OPTION_CHAINS')
futuresTableName = os.getenv('FUTURES_TABLE_NAME', 'FUTURES')
nextExpiryTransitionDate: datetime.date = None
config: dict = {}
openingPrices: dict = {}
expiryDates: list = []
currentExpiryDate = None
stocksToIgnore: list = None
activityConditions: dict = None
logging.basicConfig(level=logLevel)
db: sqlalchemy.engine.base.Engine = None 

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
    global config, openingPrices, expiryDates, currentExpiryDate, nextExpiryTransitionDate, stocksToIgnore, activityConditions
    ssm = boto3.client('ssm')
    logging.info('getting config and nse holiday list from SSM Parameter store')
    yamlConfig = ssm.get_parameter(Name=f'{ssmParameterPrefix}/{environment}/{configParameterName}')['Parameter']['Value']
    config = yaml.safe_load(yamlConfig)
    nseHolidayList = ssm.get_parameter(Name=f'{ssmParameterPrefix}/{environment}/{nseHolidaysParameterName}')['Parameter']['Value'].split(',')
    activityConditions = yaml.safe_load(ssm.get_parameter(Name=f'{ssmParameterPrefix}/{environment}/{activityConditionsParameterName}')['Parameter']['Value'])
    now = datetime.now()
    today = now.strftime('%d-%b-%Y')
    if today in nseHolidayList:
        logging.warn(f'today: {today} is NSE holiday. Exiting..')
        exit(0)
    logging.info(f'creating s3 client to upload the json data to s3 bucket: {openingPriceBucketName} key: {openingPriceFileName}')
    s3 = boto3.resource('s3')
    s3Object = s3.Object(openingPriceBucketName, openingPriceFileName)
    logging.info('getting opening prices from s3')
    resp = s3Object.get()
    if resp['ResponseMetadata']['HTTPStatusCode'] == 200:
        logging.info('fetched data from s3')
    else:
        logging.error('error getting data from s3')
    s3JsonData = json.loads(resp['Body'].read().decode('utf-8'))
    openingPrices = s3JsonData['openingPrices']
    expiryDates = s3JsonData['expiryDates']
    currentExpiryDate = datetime.strptime(expiryDates[0], '%d-%b-%y')
    nextExpiryTransitionDate = currentExpiryDate - timedelta(days=config['urlConfigs']['stocks']['options']['nextExpiryDateIncludeDays'])
    stocksToIgnore = config['stocksToIgnore']
    setDB()
    optionsData = fetchMcOptsData()
    # optionsData.to_csv('out.csv', index=False)
    updateTable(optionsData, optionsChainTableName)
    updateTable(fetchFutureData(), futuresTableName)

def setDB():
    secret_name = dbCredentialsSecretName
    region_name = dbCredentialsSecretRegion
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    global db        
    dbCredentials = json.loads(secret)
    connString = 'mysql+mysqlconnector://'+dbCredentials['username']+':'+dbCredentials['password']+'@'+dbCredentials['host']+':'+dbCredentials['port']+'/'+dbCredentials['dbname']
    db = sqlalchemy.create_engine(connString)

def updateTable(df: pd.DataFrame, tableName:str):
    with db.begin() as connection:
        df.to_sql(tableName, con=connection, if_exists='replace', index=False)

def recordOI(df: pd.DataFrame, tableName: str):
    pass

def filterAndUpdateOptionChain(df: pd.DataFrame):
    currentTime = datetime.now().time()
    writingCriteria = unwindingCriteria = None
    try:
        writingCriteria = [c for c in activityConditions['WRITINGCRITERIAS'] if datetime.strptime(c['start_time'], '%H:%M:%S').time() < currentTime and currentTime <= datetime.strptime(c['end_time'], '%H:%M:%S').time()][0]
        unwindingCriteria = [c for c in activityConditions['UNWINDINGCRITERIAS'] if datetime.strptime(c['start_time'], '%H:%M:%S').time() < currentTime and currentTime <= datetime.strptime(c['end_time'], '%H:%M:%S').time()][0]
    except IndexError:
        writingCriteria = activityConditions['WRITINGCRITERIAS'][2]
        unwindingCriteria = activityConditions['UNWINDINGCRITERIAS'][2]
    oiRecordsUpperLowerLimit = config['urlConfigs']['stocks']['options']['oi_records_upper_lower_limit']
    priceMultiple = config['urlConfigs']['stocks']['options']['price_multiple']
    symbol = df['Symbol'].unique()[0]
    newDf = pd.concat([df.loc[(df['StrikePrice'] >= openingPrices[symbol]) & (df['StrikePrice'] % priceMultiple == 0)].head(oiRecordsUpperLowerLimit), df.loc[(df['StrikePrice'] < openingPrices[symbol]) & (df['StrikePrice'] % priceMultiple == 0)].tail(oiRecordsUpperLowerLimit)]).sort_values(by='StrikePrice')
    ## CE WRITING Criteria evaluation
    newDf.loc[(writingCriteria['price_change_percent'] > pd.to_numeric(newDf['CE.pChange']))&(writingCriteria['oi_change_percent']) < pd.to_numeric(newDf['CE.pOIChange']), 'CE.WRITING'] = True
    ## PE WRITING Criteria evaluation
    newDf.loc[(writingCriteria['price_change_percent'] > pd.to_numeric(newDf['PE.pChange']))&(writingCriteria['oi_change_percent']) < pd.to_numeric(newDf['PE.pOIChange']), 'PE.WRITING'] = True
    ## CE UNWINDING Criteria evaluation
    newDf.loc[(unwindingCriteria['price_change_percent'] < pd.to_numeric(newDf['CE.pChange']))&(writingCriteria['oi_change_percent']) > pd.to_numeric(newDf['CE.pOIChange']), 'CE.UNWINDING']= True
    ## PE UNWINDING Criteria evaluation
    newDf.loc[(unwindingCriteria['price_change_percent'] < pd.to_numeric(newDf['PE.pChange']))&(writingCriteria['oi_change_percent']) > pd.to_numeric(newDf['PE.pOIChange']), 'PE.UNWINDING'] = True
    return newDf

def fetchMcOptsData() -> pd.DataFrame:
    logging.info(f'inside fetchMcOptsData to create filtered dataframe')
    mcOptsConfig = config['urlConfigs']['stocks']['options']
    mcOptsUrls = [mcOptsConfig['urlExpiry1']]
    if datetime.now() > nextExpiryTransitionDate:
        mcOptsUrls.append(mcOptsConfig['urlExpiry2'])
    mcOptsUrlHeaders = mcOptsConfig['url_headers']
    mcOptsTblAttr = mcOptsConfig['tbl_attr']
    mcOptsColsToSplit = mcOptsConfig['cols_to_split']
    rawDf: pd.DataFrame = pd.DataFrame()
    for mcOptsUrl in mcOptsUrls:
        rawDf = pd.concat([rawDf, pd.read_html(_getTable(mcOptsUrl, mcOptsUrlHeaders, mcOptsTblAttr))[0]])
    rawDf = rawDf[~rawDf['Symbol'].isin(stocksToIgnore)]
    stocksDf = _splitRows(rawDf.rename(columns=mcOptsConfig['cols_to_rename']), mcOptsColsToSplit)
    stocksDf.columns = stocksDf.columns.str.replace(r' ', '')
    sharedCols = mcOptsConfig['uniqueColumnsWithoutSpaces']
    stocksDfCE = stocksDf.loc[(stocksDf['OptionType'] == 'CE')].drop('OptionType', axis='columns').reset_index(drop=True)
    stocksDfCE = pd.concat([stocksDfCE[sharedCols], stocksDfCE[[colName for colName in stocksDfCE.columns if colName not in sharedCols]].add_prefix('CE.')], axis='columns')
    stocksDfPE = stocksDf.loc[(stocksDf['OptionType'] == 'PE')].drop('OptionType', axis='columns').reset_index(drop=True)
    stocksDfPE = pd.concat([stocksDfPE[sharedCols], stocksDfPE[[colName for colName in stocksDfPE.columns if colName not in sharedCols]].add_prefix('PE.')], axis='columns')
    stocksDfFinal = pd.merge(stocksDfCE, stocksDfPE, how='outer', on=['Symbol', 'ExpiryDate', 'StrikePrice']).sort_values(by=['Symbol', 'ExpiryDate', 'StrikePrice'], ignore_index=True)
    strikePrices = stocksDfFinal.pop('StrikePrice')
    stocksDfFinal.insert(14, 'StrikePrice', strikePrices)
    stocksDfFinal = pd.concat([stocksDfFinal, pd.DataFrame(columns=['CE.WRITING', 'PE.WRITING', 'CE.UNWINDING', 'PE.UNWINDING'], dtype=bool)], axis='columns')
    stocksDfFinalGroups = stocksDfFinal.groupby(['Symbol', 'ExpiryDate'])
    stocksDfFinalGroupsFiltered = stocksDfFinalGroups.apply(filterAndUpdateOptionChain)
    return stocksDfFinalGroupsFiltered

def fetchFutureData() -> pd.DataFrame:
    logging.info('inside fetchFutureData')
    mcFutConfig      = config['urlConfigs']['stocks']['futures']
    mcFutUrlHeaders  = mcFutConfig['url_headers']
    mcFutUrl         = mcFutConfig['url']
    mcFutTblAttr     = mcFutConfig['tbl_attr']
    mcFutColsToSplit = mcFutConfig['cols_to_split']
    rawDf: pd.DataFrame = pd.DataFrame()
    rawDf = pd.read_html(_getTable(mcFutUrl, mcFutUrlHeaders, mcFutTblAttr))[0]
    rawDf = rawDf[~rawDf['Symbol'].isin(stocksToIgnore)]
    stocksDf = _splitRows(rawDf.rename(columns=mcFutConfig['cols_to_rename']), mcFutColsToSplit)
    stocksDf.columns = stocksDf.columns.str.replace(r' ', '')
    return stocksDf

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
    Inputs a DataFrame and splits the columns into two upon ' ' delimiter. Returns deep copy object of the new DataFrame.
    
    Parameters
    ----------
    df: pd.DataFrame, required
        DataFrame to operate on.

    colsToSplit: dict, required
        columns names to splits and the new column names. 
        Example: {'High Low': ['High', 'Low'], 'Open Int Chg': ['OI Change', 'OI Change %'], 'Vol - Shares Contracts': ['Vol', 'Shares Contracts'], 'Change Chg %': ['Change', 'pChange']}
    """
    logging.info('inside _splitRows')
    df = df.copy(deep=True)
    for colName, newCols  in colsToSplit.items():
        df[newCols] = df[colName].str.split(expand=True)
        for newCol in newCols:
            df[newCol] = df[newCol].str.replace('[,%]', '')
        df = df.drop(colName, axis='columns')
    return df.copy(deep=True)
    
if __name__ == '__main__':
    start()
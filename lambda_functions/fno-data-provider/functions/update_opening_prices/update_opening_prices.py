import boto3, logging, os, yaml, json, pandas as pd, yfinance as yf
from datetime import datetime

logLevel = os.getenv('LOG_LEVEL', 'INFO').upper()
environment = os.getenv('ENVIRONMENT', 'prod')
configParameterPrefixShared = os.getenv('CONFIG_PARAMETER_PREFIX', '/fno-shared-data')
configParameterPrefix = os.getenv('CONFIG_PARAMETER_PREFIX', '/fno-stock-data-provider')
configParameterName = os.getenv('CONFIG_PARAMETER_NAME', 'data-fetcher-config')
openingPriceFileName = os.getenv('OPENING_PRICE_FILE_NAME', 'opening_prices.json')
openingPriceBucketName = os.getenv('OPENING_PRICE_BUCKET_NAME', 'fnoanalyser')
nseHolidaysParameterName = os.getenv('NSE_HOLIDAY_PARAMETER_NAME', 'nse-holiday-list')
config: dict = {}
symbolSuffix = '.NS'
logging.basicConfig(level=logLevel)
stocksToIgnore = ['ZYDUSLIFE','CADILAHC']

def lambda_handler(event, context):
  start()

def start():
  logging.info("creating SSM client to fetch the futures url from f'{configParameterPrefix}/{environment}/{configParameterName} and NSE Holiday List from f'{configParameterPrefix}/{environment}/{nseHolidaysParameterName}' parameters")
  ssm = boto3.client('ssm')
  nseHolidayList = ssm.get_parameter(Name=f'{configParameterPrefixShared}/{environment}/{nseHolidaysParameterName}')['Parameter']['Value'].split(',')
  now = datetime.now()
  today = now.strftime('%d-%b-%Y')
  logging.info('Checking if today is a holiday')
  if today in nseHolidayList:
      logging.warn(f'today: {today} is NSE holiday. Exiting..')
      exit(0)
  yamlConfig = ssm.get_parameter(Name=f'{configParameterPrefix}/{environment}/{configParameterName}')['Parameter']['Value']
  config = yaml.safe_load(yamlConfig)
  url = config['configs']['futures']['url']
  logging.debug(f'futures url: {url}')
  df = pd.read_html(url)[0]
  symbols = pd.Series(pd.unique(df['Symbol']))
  symbols = symbols[~symbols.isin(stocksToIgnore)]
  expiryDates = list(pd.unique(df['ExpiryDate'].sort_values()))
  logging.debug(f'symbols = {symbols}')
  yfTickers = yf.Tickers(f'{symbolSuffix} '.join(symbols)+symbolSuffix)
  symbolsHistoryDf = yfTickers.history(period='1d')
  logging.debug(f'symbolsHistoryDf = \n{symbolsHistoryDf}')
  symbolsOPPDf = symbolsHistoryDf['Open'].copy(deep = True)
  logging.debug(f'symbolsOPPDf = \n{symbolsOPPDf}')
  symbolsOPPDf.columns = symbolsOPPDf.columns.str.replace(symbolSuffix, '', regex=False)
  openingPricesDict = symbolsOPPDf.reset_index(drop=True).loc[0].to_dict()
  dictData = {'expiryDates': expiryDates, 'openingPrices': openingPricesDict}
  jsonData = json.dumps(dictData).encode('UTF-8')
  logging.debug(f'jsonData = \n{jsonData}')
  logging.info(f'creating s3 client to upload the json data to s3 bucket: {openingPriceBucketName} key: {openingPriceFileName}')
  s3 = boto3.resource('s3')
  s3Object = s3.Object(openingPriceBucketName, openingPriceFileName)
  result = s3Object.put(Body = jsonData)
  response = result.get('ResponseMetadata').get('HTTPStatusCode')
  if response != 200:
    logging.error('File not uploaded')
  else:
    logging.info('File uploaded successfully')


if __name__ == '__main__':
  start()
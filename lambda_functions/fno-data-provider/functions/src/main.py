import logging, os, yaml, sqlalchemy, json, pandas as pd, yfinance as yf
from datetime import datetime
from common.instrument import Instrument
from common.database import DataBase
from common.aws import AWS
from common.market import Market

def updateOpeningPrices(instrDetails: dict, **kwargs):
  instr = Instrument(instrDetails['kind'], instrDetails['configParam'], instrDetails['symbolsMetadataParam'], instrDetails['activitiesParam'])
  s3 = AWS('s3')
  openingPrices = instr.getOpeningPrices()
  if 's3BucketName' not in kwargs or 's3BucketFile' not in kwargs:
    raiseForWrongValue('both s3BucketName and s3BucketFile must be provided to updateOpeningPrices function')
  else:
    s3BucketName = kwargs['s3BucketName']
    s3BucketFile = kwargs['s3BucketFile']
  s3.uploadJsonToS3(openingPrices, s3BucketName, s3BucketFile)
  

def updateOptionChain(instrKind: str, **kwargs) -> pd.DataFrame:
  pass

def raiseForWrongValue(msg:str = 'Wrong Value Provided'):
  raise ValueError(msg)

if __name__ == '__main__':
  # # stock = Instrument('stock', '/fno-stock-data-provider/prod/data-fetcher-config', '/fno-shared-data/prod/yahoo-finance-symbols-metadata','/fno-stock-data-provider/prod/activity-conditions')
  # index = Instrument('index', '/fno-index-data-provider/prod/data-fetcher-config', '/fno-shared-data/prod/yahoo-finance-symbols-metadata')
  # # stockOPs = stock.getOpeningPrices()
  # indexOPs = index.getOpeningPrices()
  # # op = stockOPs
  # op = indexOPs
  # print(op)
  # # stockOptionsChain = stock.getOptionChain(stockOPs)
  # indexOptionsChain = index.getOptionChain(indexOPs, False)
  # # opc = stockOptionsChain
  # opc = indexOptionsChain
  # # opc.to_csv('stock_out.csv')
  # opc.to_csv('index_out.csv')
  purpose = os.getenv('PURPOSE', 'UPDATE_OPENING_PRICES')
  instrmntKind = os.getenv('INSTRUMENT_KIND', 'STOCK')
  s3BucketName = os.getenv('S3BUCKET_NAME', 'fnoanalyser')
  s3BucketFileName = os.getenv('S3BUCKET_FILE_NAME', 'opening_prices.json')
  instr: Instrument = None
  purposes = {
    'UPDATE_OPENING_PRICES': updateOpeningPrices,
    'UPDATE_OPTION_CHAIN': updateOptionChain
  }
  instrmntKinds = {
    'STOCK': 'stock',
    'INDEX': 'index'
  }
  stockDetailsDefaults = {
    'configParam': '/fno-stock-data-provider/prod/data-fetcher-config',
    'symbolsMetadataParam': '/fno-shared-data/prod/yahoo-finance-symbols-metadata',
    'activitiesParam': '/fno-stock-data-provider/prod/activity-conditions'
  }
  indexDetailsDefauls = {
    'configParam': 'fno-index-data-provider/prod/data-fetcher-config',
    'symbolsMetadataParam': '/fno-shared-data/prod/yahoo-finance-symbols-metadata',
    'activitiesParam': None
  }
  defauls: dict = None
  instrmntKind = instrmntKinds.get(instrmntKind, None)
  extraArgs = {'s3BucketName': s3BucketName, 's3BucketFile': s3BucketFileName}
  if instrmntKind is None:
    raiseForWrongValue()
  elif instrmntKind == 'stock':
    defaults = stockDetailsDefaults
  elif instrmntKind == 'index':
    defaults = indexDetailsDefauls
  instrmntDetails = {
    'kind': instrmntKind,
    'configParam': os.getenv('INSTRUMENT_CONFIG_PARAM',defaults['configParam']),
    'symbolsMetadataParam': os.getenv('SYMBOLS_METADATA_PARAM', defaults['symbolsMetadataParam']),
    'activitiesParam': os.getenv('ACTIVITIES_PARAM', defaults['activitiesParam'])
  }
  purposeFunc = purposes.get(purpose, None)
  if purposeFunc is None:
    raiseForWrongValue()
  purposeFunc(instrmntDetails, **extraArgs)
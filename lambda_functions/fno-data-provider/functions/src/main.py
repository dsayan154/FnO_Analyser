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
  

def updateOptionChain(instrDetails: dict, **kwargs):
  instr = Instrument(instrDetails['kind'], instrDetails['configParam'], instrDetails['symbolsMetadataParam'], instrDetails['activitiesParam'])
  s3BucketName = kwargs.get('s3BucketName', None)
  s3BucketFile = kwargs.get('s3BucketFile', None)
  dbSecret = kwargs.get('dbSecretName', None)
  dbTableName = kwargs.get('dbTableName', None)
  if not (s3BucketName and s3BucketFile and dbSecret and dbTableName):
    raiseForWrongValue('both s3BucketName, s3BucketFile, DBSecretName and DB_TABLE_NAME must be provided to updateOptionChain function')
  openingPrices = instr.getOpeningPrices(s3BucketName, s3BucketFile)
  keepOnlyCentralStrikes = True if instrDetails['kind'] == 'stock' else False
  ocDf = instr.getOptionChain(openingPrices,keepOnlyCentralStrikes)
  db = DataBase(json.loads(AWS('secretsmanager').getDataFromSecretsManager(dbSecret)))
  db.overwriteTable(ocDf, dbTableName)

def raiseForWrongValue(msg:str = 'Wrong Value Provided'):
  raise ValueError(msg)

if __name__ == '__main__':
  purpose = os.getenv('PURPOSE', 'UPDATE_OPTION_CHAIN')
  instrmntKind = os.getenv('INSTRUMENT_KIND', 'STOCK')
  dbDetails = {  
    "dbSecretName": os.getenv('DB_SECRET_NAME', None),
    "dbTableName": os.getenv("DB_TABLE_NAME", None)
  }
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
    'activitiesParam': '/fno-stock-data-provider/prod/activity-conditions',
    's3BucketFileName': 'stock_opening_prices.json'
  }
  indexDetailsDefauls = {
    'configParam': '/fno-index-data-provider/prod/data-fetcher-config',
    'symbolsMetadataParam': '/fno-shared-data/prod/yahoo-finance-symbols-metadata',
    'activitiesParam': None,
    's3BucketFileName': 'index_opening_prices.json'
  }
  defaults: dict = None
  instrmntKind = instrmntKinds.get(instrmntKind, None)
  if instrmntKind is None:
    raiseForWrongValue()
  elif instrmntKind == 'stock':
    defaults = stockDetailsDefaults
  elif instrmntKind == 'index':
    defaults = indexDetailsDefauls
  s3BucketName = os.getenv('S3BUCKET_NAME', 'fnoanalyser')
  s3BucketFileName = os.getenv('S3BUCKET_FILE_NAME', defaults['s3BucketFileName'])
  extraArgs = {'s3BucketName': s3BucketName, 's3BucketFile': s3BucketFileName}
  instrmntDetails = {
    'kind': instrmntKind,
    'configParam': os.getenv('INSTRUMENT_CONFIG_PARAM',defaults['configParam']),
    'symbolsMetadataParam': os.getenv('SYMBOLS_METADATA_PARAM', defaults['symbolsMetadataParam']),
    'activitiesParam': os.getenv('ACTIVITIES_PARAM', defaults['activitiesParam'])
  }
  extraArgs.update(dbDetails)
  purposeFunc = purposes.get(purpose, None)
  if purposeFunc is None:
    raiseForWrongValue()
  purposeFunc(instrmntDetails, **extraArgs)
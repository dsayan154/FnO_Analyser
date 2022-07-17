import logging, requests, math, yfinance as yf, pandas as pd
from datetime import datetime as dt
from bs4 import BeautifulSoup
from common.aws import AWS

class Instrument:
  kinds = {
    'stock': 'stock',
    'index': 'index'
  }

  def __init__(self, kind:str, configParameter:str, symbolsMetadataParameter:str, activitiesParameter:str = None):
    self.awsClient = AWS('ssm', 's3')
    self.kind = self.kinds.get(kind, 'wrongKind')
    if kind == 'wrongKind':
      raise f'wrong kind passed: expected values: {self.kinds.keys()}'
    self.details = self.awsClient.getYamlDataFromSSM(configParameter)
    self._setDetails(self.details)
    self.symbolsMetadata:dict = self.awsClient.getYamlDataFromSSM(symbolsMetadataParameter)['kinds'][kind]
    self.activityConditions:dict = self.awsClient.getYamlDataFromSSM(activitiesParameter) if activitiesParameter else None
    # self.symbols:list[str] = self._getSymbols() 
    self.rawOptionsDf = self._getDataFrame('options')
    self.rawFuturesDf = self._getDataFrame('futures')

  def _setDetails(self, details:dict):
    self.symbolsToIgnore: list[str] = details.get('symbolsToExclude', None)
    self.symbolsToConsider: list[str] = details.get('symbolsToInclude', None)
    self.symbolsExpiriesFrom: str = details.get('symbolsAndExpiryDatesFrom', None)
    if not self.symbolsExpiriesFrom:
      raise f'symbolsAndExpiryDatesFrom not set for the instrument type: {self.kind}'
    config: dict = details.get('configs', None)
    if not config:
      raise f'config for this instrument is not set'
    self.optionsConfig:dict = config.get('options', None)
    self.futuresConfig:dict = config.get('futures', None)
    if self.optionsConfig == None or self.futuresConfig == None:
      raise 'either options or futures config is missing'
  
  def _getSymbols(self) -> list[str]:
    df = self.rawFuturesDf if self.details['symbolsAndExpiryDatesFrom'] == 'futures' else self.rawOptionsDf
    return list(pd.unique(df['Symbol']))
    

  def _getDataFrame(self, instrType:str) -> pd.DataFrame:
    config:dict = self.optionsConfig if instrType == 'options' else self.futuresConfig
    # urlKey:str = self.details['symbolsExpiriesFrom']['key']
    # urlKeyValue:str = self.details['symbolsExpiriesFrom']['value']
    # url:str = self.details[urlKey][urlKeyValue]
    allUrls = config.get('urls')
    count = 0
    url = allUrls[count]
    headers:dict = config.get('url_headers', None)
    tableAttributes:dict = config.get('tbl_attr', None)
    tables = pd.read_html(self._getTable(url, headers, tableAttributes))
    df = tables[0]
    if len(allUrls) > 1 and 'daysBeforeConideringNextExpData' in config:
      currentExpiryDate = pd.unique(pd.to_datetime(df['Expiry Date'].sort_values()))[0]
      nextExpiryTransitionDate = currentExpiryDate - pd.offsets.Day(config.get('daysBeforeConideringNextExpData'))
      today = pd.Timestamp('today').floor('D')
      if today >= nextExpiryTransitionDate:
        df = pd.concat(df, pd.read_html(self._getTable(allUrls[count+1], headers, tableAttributes))[0])
    if self.symbolsToIgnore:
      df.drop(df[df['Symbol'].isin(self.symbolsToIgnore)].index, inplace=True)
    if self.symbolsToConsider:
      df.drop(df[~df['Symbol'].isin(self.symbolsToConsider)].index, inplace=True)
    df.rename(columns=config['cols_to_rename'], inplace=True)
    df = self._splitRows(df, config.get('cols_to_split'))
    df.columns = df.columns.str.replace(' ','')
    return df
  
  def _splitRows(self, df: pd.DataFrame, colsToSplit: dict) -> pd.DataFrame:
    """
    Inputs a DataFrame and splits the columns according to the colsToSplit dict. Returns deep copy object of the new DataFrame.
    
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
  
  def _getTable(self, url: str, urlHeaders: dict, attrs: dict) -> str:
    """
    fetches a html code of the table with attribute attrs from the input url
    """
    logging.debug(f'inside _getTable for {url}')
    response = requests.get(url, headers=urlHeaders)
    if response.status_code != 200:
        logging.warn(f'{url} returned unexpected status code: {response.status_code}')
    soup = BeautifulSoup(response.text, features='lxml')
    table = soup.find('table', attrs=attrs)
    for tr in table.find_all('tr'):
      for br in tr.find_all('br'):
        br = br.replace_with(' ')
    return str(table)

  def getOpeningPrices(self, s3BucketName:str=None, s3BucketFileName:str=None) -> dict:
    openingPricesDict: dict = None
    if s3BucketName and s3BucketFileName:
      openingPricesDict = self.awsClient.getJsonDataFromS3(s3BucketName, s3BucketFileName)
    else:
      yfTickers:str = ''
      symbols = self._getSymbols()
      suffix: str = None
      if 'suffix' in self.symbolsMetadata:
        suffix = self.symbolsMetadata["suffix"]
        yfTickers = f'{suffix} '.join(symbols) + suffix
      else:
        for symbol in symbols:
          yfTickers += self.symbolsMetadata[symbol] + ' '
      tickers = yf.Tickers(yfTickers.rstrip())
      openingPricesDf: pd.DataFrame = tickers.history(period='1d')['Open'].reset_index(drop=True)
      if 'suffix' in self.symbolsMetadata:
        openingPricesDf.columns = openingPricesDf.columns.str.removesuffix(suffix)
      else:
        newMetadataDict = dict([(val, key) for key, val in self.symbolsMetadata.items()])
        openingPricesDf.rename(columns=newMetadataDict, inplace=True)
      openingPricesDict = openingPricesDf.loc[0].to_dict()
    return openingPricesDict
  
  def getExpiryDates(self, s3BucketName:str=None, s3BucketFileName:str=None) -> tuple:
    df = self.rawFuturesDf if self.symbolsExpiriesFrom == 'futures' else self.rawOptionsDf
    expiryDates: list = None
    config = self.futuresConfig if self.symbolsExpiriesFrom == 'futures' else self.optionsConfig
    if not (s3BucketName and s3BucketFileName):
      expiryDates = list(pd.unique(pd.to_datetime(df['ExpiryDate'].sort_values())))
    else:
      s3JsonData = self.awsClient.getJsonDataFromS3(s3BucketName, s3BucketFileName)
      expiryDates = s3JsonData['expiryDates']
    currentExpiryDate = expiryDates[0]
    nextExpiryTransitionDate = None if 'nextExpiryDateIncludeDays' in config else currentExpiryDate - pd.offsets.Day(config.get('nextExpiryDateIncludeDays'))
    return (expiryDates, currentExpiryDate, nextExpiryTransitionDate)
  
  def getOptionChain(self, openingPrices:dict, keepOnlyCentralStrikes:bool=True) -> pd.DataFrame:
    logging.info(f'Opening Prices: {openingPrices}')
    df = self.rawOptionsDf.copy(deep=True)
    df.columns = df.columns.str.replace(r' ','')
    config = self.optionsConfig
    sharedCols = config['uniqueColumnsWithoutSpaces']
    dfCE = df.loc[(df['OptionType'] == 'CE')].drop('OptionType', axis='columns').reset_index(drop=True)
    dfCE = pd.concat([dfCE[sharedCols], dfCE[[colName for colName in dfCE.columns if colName not in sharedCols]].add_prefix('CE.')], axis='columns')
    dfPE = df.loc[(df['OptionType'] == 'PE')].drop('OptionType', axis='columns').reset_index(drop=True)
    dfPE = pd.concat([dfPE[sharedCols], dfPE[[colName for colName in dfPE.columns if colName not in sharedCols]].add_prefix('PE.')], axis='columns')
    dfFinal = pd.merge(dfCE, dfPE, how='outer', on=['Symbol', 'ExpiryDate', 'StrikePrice']).sort_values(by=['Symbol', 'ExpiryDate', 'StrikePrice'], ignore_index=True)
    strikePrices = dfFinal.pop('StrikePrice')
    dfFinal.insert(14, 'StrikePrice', strikePrices)
    if self.activityConditions:
      dfFinal = pd.concat([dfFinal, pd.DataFrame(columns=['CE.WRITING', 'PE.WRITING', 'CE.UNWINDING', 'PE.UNWINDING'], dtype=bool)], axis='columns')
    dfFinalGroups = dfFinal.groupby(['Symbol', 'ExpiryDate'])
    dfFinal = dfFinalGroups.apply(self._filterCentralStrikes, openingPrices, keepOnlyCentralStrikes)
    dfFinal = dfFinal if not self.activityConditions else self._updateActivities(dfFinal)
    return dfFinal.reset_index(drop=True)

  def _filterCentralStrikes(self, df: pd.DataFrame, openingPrices:dict, keepOnlyCentralStrikes:bool=True):
    config = self.optionsConfig
    oiRecordsUpperLowerLimitPercent = config.get('oi_records_upper_lower_limit_percent', 100)
    priceMultiple = config['price_multiple']
    symbol = df['Symbol'].unique()[0]
    strikePriceDelta: int = int((openingPrices[symbol]*oiRecordsUpperLowerLimitPercent)/100)
    strikePriceUpperLimit: int = openingPrices[symbol] + strikePriceDelta
    strikePriceLowerLimit: int = openingPrices[symbol] - strikePriceDelta
    strikePrices: pd.Series = df[(strikePriceLowerLimit <= df['StrikePrice'])&(df['StrikePrice'] <= strikePriceUpperLimit) & (df['StrikePrice'] % priceMultiple == 0)]['StrikePrice']
    if not keepOnlyCentralStrikes:
      df = pd.concat([df, pd.DataFrame(columns=['Display'], dtype=bool)], axis='columns')
      df.loc[df['StrikePrice'].isin(strikePrices),['Display']] = True
    else:
      df = df[df['StrikePrice'].isin(strikePrices)]
    df.sort_values(by=['StrikePrice'], inplace=True)
    return df

  def _updateActivities(self, df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy(deep=True)
    currentTime = dt.now().time()
    writingCriteria = unwindingCriteria = None
    try:
        writingCriteria = [c for c in self.activityConditions['WRITINGCRITERIAS'] if dt.strptime(c['start_time'], '%H:%M:%S').time() < currentTime and currentTime <= dt.strptime(c['end_time'], '%H:%M:%S').time()][0]
        unwindingCriteria = [c for c in self.activityConditions['UNWINDINGCRITERIAS'] if dt.strptime(c['start_time'], '%H:%M:%S').time() < currentTime and currentTime <= dt.strptime(c['end_time'], '%H:%M:%S').time()][0]
    except IndexError:
        writingCriteria = self.activityConditions['WRITINGCRITERIAS'][2]
        unwindingCriteria = self.activityConditions['UNWINDINGCRITERIAS'][2]
    ## CE WRITING Criteria evaluation
    df.loc[(writingCriteria['price_change_percent'] > pd.to_numeric(df['CE.pChange']))&(writingCriteria['oi_change_percent']) < pd.to_numeric(df['CE.pOIChange']), 'CE.WRITING'] = True
    ## PE WRITING Criteria evaluation
    df.loc[(writingCriteria['price_change_percent'] > pd.to_numeric(df['PE.pChange']))&(writingCriteria['oi_change_percent']) < pd.to_numeric(df['PE.pOIChange']), 'PE.WRITING'] = True
    ## CE UNWINDING Criteria evaluation
    df.loc[(unwindingCriteria['price_change_percent'] < pd.to_numeric(df['CE.pChange']))&(writingCriteria['oi_change_percent']) > pd.to_numeric(df['CE.pOIChange']), 'CE.UNWINDING']= True
    ## PE UNWINDING Criteria evaluation
    df.loc[(unwindingCriteria['price_change_percent'] < pd.to_numeric(df['PE.pChange']))&(writingCriteria['oi_change_percent']) > pd.to_numeric(df['PE.pOIChange']), 'PE.UNWINDING'] = True
    return df

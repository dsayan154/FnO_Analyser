import logging
from conditions.conditions import CRITERIASTYPES
import pandas as pd
import time, copy
import numpy as np

def calculateAndUpdateOptionChainDf(stockName: str, optionsDf: pd.DataFrame) -> pd.DataFrame:
    '''
    Updates the optionsDf after making calculation on the optionsDf based on writing and unwinding criterias mentioned in the conditions.condtions module.
    '''
    logging.debug('inside calculateAndUpdate')
    optionsDf: pd.DataFrame = optionsDf.copy(deep=True)
    logging.debug(f'optionsDf: \n{optionsDf}')
    criteriaTypes = copy.deepcopy(CRITERIASTYPES)
    for criteriaType in criteriaTypes:
        for criteria in criteriaType:
            now = time.localtime()
            criteria['start_time'] = time.mktime((now[0], now[1], now[2], criteria['start_time'][0], criteria['start_time'][1], criteria['start_time'][2], now[6], now[7], now[8]))
            criteria['end_time'] = time.mktime((now[0], now[1], now[2], criteria['end_time'][0], criteria['end_time'][1], criteria['end_time'][2], now[6], now[7], now[8]))
    logging.debug(f'criterias after time updation: {criteriaTypes}')
    peWriting = []
    ceWriting  = []
    peUnwinding = []
    ceUnwinding = []
    for _,row in optionsDf.iterrows():
        writingCriteria = criteriaTypes[0][2]
        unwindingCriteria = criteriaTypes[1][2]
        try:
            writingCriteria = [c for c in criteriaTypes[0] if c['start_time'] < time.time() and time.time() <= c['end_time']][0]
            unwindingCriteria = [c for c in criteriaTypes[1] if c['start_time'] < time.time() and time.time() <= c['end_time']][0]
        except IndexError:
            pass
        if float(writingCriteria['price_change_percent'])>float(row['PE.pChange']) and float(writingCriteria['oi_change_percent']<float(row['PE.pchangeinOpenInterest'])):
            peWriting.append('TRUE')
        else:
            peWriting.append('FALSE')
        if float(writingCriteria['price_change_percent'])>float(row['CE.pChange']) and float(writingCriteria['oi_change_percent']<float(row['CE.pchangeinOpenInterest'])):
            ceWriting.append('TRUE')
        else:
            ceWriting.append('FALSE')
        if float(unwindingCriteria['price_change_percent'])<float(row['PE.pChange']) and float(unwindingCriteria['oi_change_percent']>float(row['PE.pchangeinOpenInterest'])):
            peUnwinding.append('TRUE')
        else:
            peUnwinding.append('FALSE')
        if float(unwindingCriteria['price_change_percent'])<float(row['CE.pChange']) and float(unwindingCriteria['oi_change_percent']>float(row['CE.pchangeinOpenInterest'])):
            ceUnwinding.append('TRUE')
        else:
            ceUnwinding.append('FALSE')
    optionsDf['PE_WRITING']=peWriting
    optionsDf['CE_WRITING']=ceWriting
    optionsDf['PE_UNWINDING']=peUnwinding
    optionsDf['CE_UNWINDING']=ceUnwinding
    return optionsDf

def createOptionsDashboardDf(symbol: str, optionsChainDf:pd.DataFrame, optionsDf: pd.DataFrame) -> pd.DataFrame:
    '''
    Returns a dataframe for a stock 'symbol' by collating data from optionsChainDf, optionsDf, futuresDf and supportsResistance dict; that needs to be entered in the dashboard sheet
    '''
    filteredOptionsChainDf = optionsChainDf[(optionsChainDf['PE_WRITING'] == 'TRUE') | (optionsChainDf['CE_WRITING'] == 'TRUE') | (optionsChainDf['PE_UNWINDING'] == 'TRUE') | (optionsChainDf['CE_UNWINDING'] == 'TRUE')].copy(deep=True)
    filteredOptionsChainDf.loc[filteredOptionsChainDf['PE_WRITING'] == 'TRUE', 'PE_WRITING'] = 'PE_WRITING'
    filteredOptionsChainDf.loc[filteredOptionsChainDf['PE_WRITING'] == 'FALSE', 'PE_WRITING'] = np.NaN
    filteredOptionsChainDf.loc[filteredOptionsChainDf['PE_UNWINDING'] == 'TRUE', 'PE_UNWINDING'] = 'PE_UNWINDING'
    filteredOptionsChainDf.loc[filteredOptionsChainDf['PE_UNWINDING'] == 'FALSE', 'PE_UNWINDING'] = np.NaN
    filteredOptionsChainDf.loc[filteredOptionsChainDf['CE_WRITING'] == 'TRUE', 'CE_WRITING'] = 'CE_WRITING'
    filteredOptionsChainDf.loc[filteredOptionsChainDf['CE_WRITING'] == 'FALSE', 'CE_WRITING'] = np.NaN
    filteredOptionsChainDf.loc[filteredOptionsChainDf['CE_UNWINDING'] == 'TRUE', 'CE_UNWINDING'] = 'CE_UNWINDING'
    filteredOptionsChainDf.loc[filteredOptionsChainDf['CE_UNWINDING'] == 'FALSE', 'CE_UNWINDING'] = np.NaN
    filteredOptionsChainDf.insert(0, 'Symbol', [symbol]*len(filteredOptionsChainDf))
    filteredOptionsChainDf = filteredOptionsChainDf.rename(columns={'PE_WRITING': 'Activity1', 'CE_WRITING': 'Activity2', 'PE_UNWINDING': 'Activity3', 'CE_UNWINDING': 'Activity4', 'strikePrice': 'Strike Price'}).reset_index(drop=True)
    # filteredOptionsChainDf.sort_values('Strike Price')
    filteredOptionsChainDf = filteredOptionsChainDf[['Symbol', 'Strike Price', 'Activity1', 'Activity2', 'Activity3', 'Activity4']]
    optionsDf = optionsDf[(optionsDf['Symbol'] == symbol)&(optionsDf['Strike Price'].isin(filteredOptionsChainDf['Strike Price']))][['Strike Price','Option Type','Value (Rs. Lakh)']].reset_index(drop=True).copy(deep=True)
    optionsDfPeValue = optionsDf[(optionsDf['Option Type'] == 'PE')].rename(columns={'Value (Rs. Lakh)': 'PE.Value'}).drop('Option Type', axis='columns').reset_index(drop=True)
    optionsDfCeValue = optionsDf[(optionsDf['Option Type'] == 'CE')].rename(columns={'Value (Rs. Lakh)': 'CE.Value'}).drop('Option Type', axis='columns').reset_index(drop=True)
    optionsDfCePeValue = optionsDfCeValue.join(optionsDfPeValue.set_index('Strike Price'), on='Strike Price', how='left')
    dashboardDf = filteredOptionsChainDf.join(optionsDfCePeValue.set_index('Strike Price'), on='Strike Price', how='left')
    dashboardDf.loc[(dashboardDf['Activity1'] != 'PE_WRITING')&(dashboardDf['Activity3'] != 'PE_UNWINDING'), 'PE.Value'] = np.NaN
    dashboardDf.loc[(dashboardDf['Activity2'] != 'CE_WRITING')&(dashboardDf['Activity4'] != 'CE_UNWINDING'), 'CE.Value'] = np.NaN
    return dashboardDf

def createFuturesDashboardDf(symbol: str, mcFutDf: pd.DataFrame, supportResistancePrices: dict):
    futDashboardDf = pd.DataFrame(
                    {
                        'SYMBOL'                        : symbol, 
                        'LTP'                           : mcFutDf[(mcFutDf['Symbol'] == symbol)]['Last Price'],
                        'Price Change'                  : mcFutDf[(mcFutDf['Symbol'] == symbol)]['Change'],
                        'HIGH'                          : mcFutDf[(mcFutDf['Symbol'] == symbol)]['High'],
                        'LOW'                           : mcFutDf[(mcFutDf['Symbol'] == symbol)]['Low'], 
                        'FUTURE OI CHG%'                : mcFutDf[(mcFutDf['Symbol'] == symbol)]['OI Change %'], 
                        'SUPPORT1'                      : supportResistancePrices['support1'],
                        'SUPPORT2'                      : supportResistancePrices['support2'], 
                        'RESISTANCE1'                   : supportResistancePrices['resistance1'],
                        'RESISTANCE2'                   : supportResistancePrices['resistance2']
                    }).reset_index(drop=True)
    return futDashboardDf

def appendToDashboardDF(existingDashboardDf: pd.DataFrame, symbol:str, rawDf: pd.DataFrame, ):
    '''
    Returns a dataframe concatinating an existing dataframe with filtered rows of either PE_WRITING, CE_WRITING, PE_UNWINDING, CE_UNWINDING set to 'TRUE' in rawDf alongwith the stock symbol, highest and 2nd highest tradevolumes CE/PR strike prices. This function can accept an empty dataframe as well.
    '''
    logging.debug('inside appendToDashboardDF')
    logging.debug(f'existingDashboardDf: \n{existingDashboardDf}')
    logging.debug(f'rawDf: \n{rawDf}')
    filteredData = rawDf[(rawDf['PE_WRITING'] == 'TRUE') | (rawDf['CE_WRITING'] == 'TRUE') | (rawDf['PE_UNWINDING'] == 'TRUE') | (rawDf['CE_UNWINDING'] == 'TRUE')].copy(deep=True).filter(['strikePrice', 'PE_WRITING', 'PE_UNWINDING', 'CE_WRITING', 'CE_UNWINDING'], axis=1).copy(deep=True)
    logging.debug(f'filteredData with only TRUE criterias: \n{filteredData}\n{filteredData.info()}')
    filteredData.loc[filteredData['PE_WRITING'] == 'TRUE', 'PE_WRITING'] = 'PE_WRITING'
    filteredData.loc[filteredData['PE_WRITING'] == 'FALSE', 'PE_WRITING'] = np.NaN
    filteredData.loc[filteredData['PE_UNWINDING'] == 'TRUE', 'PE_UNWINDING'] = 'PE_UNWINDING'
    filteredData.loc[filteredData['PE_UNWINDING'] == 'FALSE', 'PE_UNWINDING'] = np.NaN
    filteredData.loc[filteredData['CE_WRITING'] == 'TRUE', 'CE_WRITING'] = 'CE_WRITING'
    filteredData.loc[filteredData['CE_WRITING'] == 'FALSE', 'CE_WRITING'] = np.NaN
    filteredData.loc[filteredData['CE_UNWINDING'] == 'TRUE', 'CE_UNWINDING'] = 'CE_UNWINDING'
    filteredData.loc[filteredData['CE_UNWINDING'] == 'FALSE', 'CE_UNWINDING'] = np.NaN
    logging.debug(f'filteredData after changing TRUE to column names: \n{filteredData}')
    filteredData.insert(0, 'Symbol', [symbol]*len(filteredData))
    # filteredData['Support 1'] = supprtResistancePrices['pePrices'][1]
    # filteredData['Support 2'] = supprtResistancePrices['pePrices'][0]
    # filteredData['Resistance 1'] = supprtResistancePrices['cePrices'][0]
    # filteredData['Resistance 2'] = supprtResistancePrices['cePrices'][1]
    filteredData.rename(columns={'strikePrice': 'Strike Price'})
    logging.debug(f'final filteredData: \n{filteredData}')
    finalDf = existingDashboardDf.append(filteredData, ignore_index=True)
    logging.debug(f'final dashboard df before returning: \n{finalDf}')
    return finalDf

def getSupportResistancePricesCePe(optsDf: pd.DataFrame) -> dict:
    '''
    Returns a dict containing support and resistance strike prices based on highest and second highest CE/PE volumes, takes the options chain dataframe as input.
    '''
    logging.debug('inside getSupportResistancePricesCePe')
    logging.debug(f'dataframe to parse: \n{optsDf}\n{optsDf.info()}')
    pePrices = optsDf.loc[optsDf['PE.totalTradedVolume'].isin(optsDf['PE.totalTradedVolume'].nlargest(2))]['strikePrice'].tolist()
    cePrices = optsDf.loc[optsDf['CE.totalTradedVolume'].isin(optsDf['CE.totalTradedVolume'].nlargest(2))]['strikePrice'].tolist()
    supprtResistancePrices = {'support1': pePrices[1], 'support2': pePrices[0],'resistance1': cePrices[0], 'resistance2': cePrices[1]}
    logging.debug(f'support and resistance prices fetched: {supprtResistancePrices}')
    return supprtResistancePrices
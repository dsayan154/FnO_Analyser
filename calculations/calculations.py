import logging
from conditions.conditions import CRITERIASTYPES
import pandas as pd
import time, copy
import numpy as np

def calculateAndUpdate(optionsDf: pd.DataFrame) -> pd.DataFrame:
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

def appendToDashboardDF(existingDashboardDf: pd.DataFrame, symbol:str, rawDf: pd.DataFrame):
    '''
    Returns a dataframe concatinating an existing dataframe with filtered rows of either PE_WRITING, CE_WRITING, PE_UNWINDING, CE_UNWINDING set to 'TRUE' in rawDf. This function can accept an empty dataframe as well.
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
    filteredData.rename(columns={'strikePrice': 'Strike Price'})
    logging.debug(f'final filteredData: \n{filteredData}')
    finalDf = existingDashboardDf.append(filteredData, ignore_index=True)
    logging.debug(f'final dashboard df before returning: \n{finalDf}')
    return finalDf
    
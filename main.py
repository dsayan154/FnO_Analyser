from datetime import datetime
import logging, yaml, time, os, traceback, pandas as pd
from calculations.calculations import *
from market import nse, moneycontrol
from xl_io.xlreadwrite import *

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
errorCount, maxErrorCount = 0, 5

inputFile = os.path.join(os.getcwd(),'configs','config.yaml')
defaultInput = {
        'output_excel_file': 'Output.xlsx',
        'run_interval_mins': 3, 
        'expiry_date': '',
        'stocks':{
            'names': [],
            'price_multiple': 1,
            'oi_records_upper_lower_limit': 10
        }
    # 'indices':{
    #     'names': [],
    #     'price_multiple': 1,
    #     'oi_records_upper_lower': 10
    # }
    }
input:dict={}

def setInput():
    global input
    try:
        with open(inputFile, 'r') as f:
            input = defaultInput.copy()
            tmpInput = dict(yaml.safe_load(f))
            logging.debug(f'yaml data to dict: {tmpInput}')
            for key in defaultInput.keys():
                if key in tmpInput:
                    input[key] = tmpInput[key]
    except Exception as e:
        logging.warning(f'{e}')

def processInput():
    logging.info('Inside processInput()')
    expiryDate = datetime.strptime(input['expiry_date'],'%d-%b-%y').date()
    stockNames = input['stocks']['names']
    logging.debug(f'stock names: {stockNames}')
    outputFile = input['output_excel_file']
    logging.debug(f'outputFile: {outputFile}')
    logging.info(f'calling getDataFrame to get Options data from moneycontrol')
    mcOptsDf = moneycontrol.getDataFrame('options', input['expiry_date'])
    logging.info(f'calling getDataFrame to get Futures data from moneycontrol')
    mcFutDf = moneycontrol.getDataFrame('futures', input['expiry_date'])
    optionsDashboardDf = futuresDashboardDf = pd.DataFrame()
    for stockName in stockNames:
        logging.debug(f'calling getOptionChain for {stockName}')
        optionsChainData = nse.getOptionChain(stockName, expiryDate, recordsLimitUpperLower=input['stocks']['oi_records_upper_lower_limit'], priceMultiple=input['stocks']['price_multiple'])
        logging.debug(f'calling calculateAndUpdate for {stockName}')
        updatedDf = calculateAndUpdateOptionChainDf(stockName, optionsChainData)
        logging.debug(f'calling getSupportResistancePricesCePe for {stockName}')
        supportResistancePrices = getSupportResistancePricesCePe(updatedDf)
        logging.debug(f'calling createDashboardDf for {stockName}')
        optionsDashboardDf = pd.concat([optionsDashboardDf, createOptionsDashboardDf(stockName, updatedDf, mcOptsDf)])
        logging.debug(f'calling appendToDashboardDF for {stockName}')
        futuresDashboardDf = pd.concat([futuresDashboardDf, createFuturesDashboardDf(stockName, mcFutDf, supportResistancePrices)]).sort_values(by='SYMBOL')
        # dashBoardDf = appendToDashboardDF(dashBoardDf, stockName, updatedDf, supportResistancePrices)
        logging.debug(f'calling createUpdateSheet for {stockName}')
        createUpdateSheet(outputFile, stockName, updatedDf, startCell='A2')
    logging.debug(f'calling createUpdateDashboardSheet')
    createUpdateDashboardSheet(outputFile, 'Dashboard', optionsDashboardDf)
    createUpdateDashboardSheet(outputFile, 'Dashboard', futuresDashboardDf,False,'K2')
    createUpdateSheet(input['output_excel_file'], 'Stock Options', mcOptsDf, startCell='A2')

if __name__ == '__main__':
    while True:
        try:
            logging.info(f'setting input from input file: {inputFile}')
            setInput()
            mktStatus = nse.capitalMarketStatus()
            logging.debug(f'{mktStatus}')
            if "Close" in mktStatus['marketStatus']:
                logging.info(f'starting to process input')
                processInput()
                logging.info(f'getting stock options from moneycontrol and writing as a sheet in excel')
            else:
                logging.error(f'Capital market is closed, next trade date is {mktStatus["tradeDate"]}, market starts at 9:15 am.')
                break
        except Exception:
            logging.warning(f'error has occured at: {datetime.now().time()}, error: \n{traceback.format_exc()}')
            errorCount += 1
            logging.warning(f'increasing error count to {errorCount}/{maxErrorCount}')
            if errorCount >= maxErrorCount:
                logging.warning('Max error count reached, will throw error and exit.')
                logging.error(f'{traceback.format_exc()}')  
        finally:
            logging.info(f'sleeping')
            # logging.debug(f'input: \n{input}')
            time.sleep(int(input['run_interval_mins'])*60)


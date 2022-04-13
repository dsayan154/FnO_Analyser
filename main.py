import logging, yaml, time, os
from calculations.calculations import *
from market.nse import *
from xl_io.xlreadwrite import *

logging.basicConfig(level=logging.DEBUG)

inputFile = os.path.join(os.getcwd(),'configs','config.yaml')
defaultInput = {
    'output_excel_file': 'Output.xlsx',
    'run_interval_mins': 3, 
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
    stockNames = input['stocks']['names']
    logging.debug(f'stock names: {stockNames}')
    dashBoardDf = pd.DataFrame()
    outputFile = input['output_excel_file']
    logging.debug(f'outputFile: {outputFile}')
    for stockName in stockNames:
        logging.debug(f'calling getOptionChain for {stockName}')
        optionsData = getOptionChain(stockName, recordsLimitUpperLower=input['stocks']['oi_records_upper_lower_limit'], priceMultiple=input['stocks']['price_multiple'])
        logging.debug(f'calling calculateAndUpdate for {stockName}')
        updatedDf = calculateAndUpdate(optionsData)
        logging.debug(f'calling appendToDashboardDF for {stockName}')
        dashBoardDf = appendToDashboardDF(dashBoardDf, stockName, updatedDf)
        logging.debug(f'calling createUpdateSheet for {stockName}')
        createUpdateSheet(outputFile, stockName, updatedDf)
    logging.debug(f'calling createUpdateDashboardSheet')
    createUpdateDashboardSheet(outputFile, 'Dashboard', dashBoardDf)

if __name__ == '__main__':
    try:
        while True:
            mktStatus = capitalMarketStatus()
            logging.debug(f'{mktStatus}')
            if mktStatus['marketStatus'] != 'Closed':
                logging.info(f'setting input from input file: {inputFile}')
                setInput()
                logging.info(f'starting to process input')
                processInput()
                logging.info(f'sleeping')
                time.sleep(input['run_interval_mins']*60)
            else:
                logging.error(f'Capital market is closed, next trade date is {mktStatus["tradeDate"]}, market starts at 9:15 am.')
                break
    except Exception as e:
        logging.error(f'{e.with_traceback()}')   

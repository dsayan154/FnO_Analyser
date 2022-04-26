from pynse import *
import pandas as pd
import logging, datetime, traceback

logging.basicConfig(level=logging.DEBUG)

nse = Nse()
def capitalMarketStatus() -> dict:
    '''
    returns the current status of the Capital Market(NIFTY) in form of dict which contains other information like next trading date, etc
    '''
    # nse.clear_data()
    logging.debug('inside capitalMarketStatus')
    try:
        marketStatus = [market for market in nse.market_status()['marketState'] if market['market'] == 'Capital Market']
        if len(marketStatus) != 0:
            return marketStatus[0]
        else:
            raise ValueError(f'Capital Market not found in market status. ')
    except ValueError as e:
        raise e
    except Exception as e:
        logging.debug(f'error occurred in getOptionChain: {e.with_traceback()}')
        pass

def getOptionChain(symbol:str, expiryDate: datetime.date, recordsLimitUpperLower: int = 10, priceMultiple:int = 1) -> pd.DataFrame:
    '''
    returns recordsLimitUpperLower number of options chains records for a stock symbol which are greater than its opening price and recordsLimitUpperLower number of options chains records for a stock symbol which are lesser than its opening price.
    '''
    logging.debug('inside getOptionChain')
    colsToDrop = ['CE.strikePrice','CE.expiryDate','CE.identifier','CE.underlying','CE.totalBuyQuantity','CE.totalSellQuantity','CE.bidQty','CE.bidprice','CE.askQty','CE.askPrice','PE.strikePrice','PE.expiryDate','PE.identifier','PE.underlying','PE.totalBuyQuantity','PE.totalSellQuantity','PE.bidQty','PE.bidprice','PE.askQty','PE.askPrice']
    filteredDf= pd.DataFrame()
    try:
        openPrice = nse.get_quote(symbol)['open']
        allChains = nse.option_chain(symbol, expiry=expiryDate).drop(colsToDrop, axis=1)
        filteredDf = pd.concat([filteredDf,allChains[(allChains['strikePrice'] > openPrice) & (allChains['strikePrice'] % priceMultiple == 0)].head(recordsLimitUpperLower), allChains[(allChains['strikePrice'] <= openPrice) & (allChains['strikePrice'] % priceMultiple == 0)].tail(recordsLimitUpperLower)])
        filteredDf['expiryDate'] = pd.to_datetime(filteredDf['expiryDate'], format='%Y-%m-%d')
        filteredDf['expiryDate'] = filteredDf['expiryDate'].astype(str)
    except Exception as e:
        logging.debug(f'error occurred in getOptionChain: {traceback.format_exc()}')
        pass
    return filteredDf.reset_index(drop=True)
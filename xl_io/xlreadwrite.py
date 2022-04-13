import logging, xlsxwriter
import xlwings as xw
import pandas as pd
import datetime as dt
from os.path import exists

LOGLEVEL=logging.WARN

logging.basicConfig(level=LOGLEVEL)

def createUpdateSheet(outputFile: str, sheetName: str, df: pd.DataFrame, startCell: str = 'A1') -> None:
    '''
    Creates/Updates excel sheet with the df DataFrame starting from cell startCell. Returns FileExists error if inputFile does not exist.
    '''
    logging.debug(f'inside createUpdateSheet')
    logging.debug(f'dataframe data: \n{df}')
    logging.debug(f'dataframe info: \n{df.info()}')
    if not exists(outputFile):
        logging.debug(f'{outputFile} does not exist, creating now.')
        wb = xlsxwriter.Workbook(outputFile)
        wb.close()
    try:
        wb = xw.Book(outputFile)
        sh = None
        try:
            sh = wb.sheets.add(sheetName)
        except ValueError as e:
            logging.warn(f'{e}')
            sh = wb.sheets(sheetName)
        sh.range(startCell).options(pd.DataFrame, index=False, dates=False).value = df
        logging.info(f'updated sheet: {sh.name}')
        wb.save()
        logging.info(f'saved workbook: {outputFile}')
    except Exception as e:
        raise e

def createUpdateDashboardSheet(outputFile: str, sheetName: str, df: pd.DataFrame) -> None:
    '''
    Creates or updates the dashboard sheet with the activity in stocks mentioned in the df dataframe. Returns FileExists error if inputFile does not exist.
    '''
    logging.debug('inside createUpdateDashboardSheet')
    logging.debug(f'sheet name: {sheetName}')
    logging.debug(f'dataframe to be written: \n{df}')
    fileExists = exists(outputFile)
    if not fileExists:
        logging.debug(f'{outputFile} does not exist, creating now.')
        wb = xlsxwriter.Workbook(outputFile)
        logging.debug(f'{outputFile} does not exist, creating now.')
        wb.close()
    try:
        wb = xw.Book(outputFile)
        sh = None
        try:
            sh = wb.sheets.add(sheetName)
        except ValueError as e:
            logging.warn(f'{e}')
            sh = wb.sheets(sheetName)
        sh.range('A2').options(pd.DataFrame, index=False, header=False).value = df
        # sh.range('C1:F1').merge()
        sh.range('A1').value = ['SYMBOL', 'STRIKE PRICE', 'ACTIVITY', 'ACTIVITY', 'ACTIVITY', 'ACTIVITY']
        # sh.range('A1').expand('right').api.HorizontalAlignment = xw.constants.HAlign.xlHAlignCenter
        sh.tables.add(sh.range('A1').expand('table'), table_style_name='TableStyleMedium13', has_headers=True, )
        logging.info('Dashboard sheet updated')
        wb.save()
        logging.info(f'Workbook saved: {sh.name}')
    except FileExistsError as e:
        raise e
    except Exception as e:
        raise e

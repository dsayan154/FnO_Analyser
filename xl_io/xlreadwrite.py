import logging, xlsxwriter
from turtle import update
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
    timeStampCell = startCell
    startCol = startCell[0]
    startRow = str(int(startCell[1])+1)
    startCell = startCol + startRow
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
        logging.info(f'updating timestamp for sheet: {sheetName}')
        sh[timeStampCell].value = f'As on: {dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}'
        sh[timeStampCell].wrap_text = True
        logging.info(f'saved workbook: {outputFile}')
    except Exception as e:
        logging.debug(f'error occured for sheet: {sheetName}')
        raise e
    finally:
        wb.save()
    #     wb.close()

def createUpdateDashboardTable(outputFile: str, sheetName: str, tableName: str, startCell: str, dataFrame: pd.DataFrame) -> None:
    '''
    Creates or updates the dashboard table in a sheet with the data in dataFrame.
    '''
    logging.debug('inside createUpdateDashboardTable')
    logging.debug(f'sheet name: {sheetName}')
    logging.debug(f'dataframe to be written: \n{dataFrame}')
    fileExists = exists(outputFile)
    timeStampCell = startCell
    startCol = startCell[0]
    startRow = str(int(startCell[1])+1)
    startCell = startCol + startRow
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
        dataFrame = dataFrame.set_index(dataFrame.columns[0])
        if tableName in [table.name for table in sh.tables]:
            sh.tables[tableName].update(dataFrame)
        else:
            newTable = sh.tables.add(source=sh[startCell],
                                    name=tableName).update(dataFrame)        
        # sh.range(startCell).options(pd.DataFrame, index=False, header=True).value = dataFrame
        # sh.range('C1:F1').merge()
        # sh.range('A1').value = ['SYMBOL', 'STRIKE PRICE', 'ACTIVITY', 'ACTIVITY', 'ACTIVITY', 'ACTIVITY', 'SUPPORT 1', 'SUPPORT 2', 'RESISTANCE 1', 'RESISTANCE 2']
        # sh.range('A1').expand('right').api.HorizontalAlignment = xw.constants.HAlign.xlHAlignCenter
        # sh.tables.add(sh.range(startCell).expand('table'), table_style_name='TableStyleMedium13', has_headers=True, )
        logging.info(f'Dashboard sheet updated: {tableName}')
        logging.info(f'updating timestamp for sheet: {sheetName}')
        sh[timeStampCell].value = f'As on: {dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}'
        sh[timeStampCell].wrap_text = True
        logging.info(f'Workbook saved: {sh.name}')
    except FileExistsError as e:
        raise e
    except Exception as e:
        raise e
    finally:
        wb.save()
    #     wb.close()

# def _mergeSymbolCol(outputFile: str, sheetName: str, symbol: str, symbolColumn: str = 'A', symbolValueStartRow: int=2) -> None:
#     '''
#     Merge cells for stock 'symbol' with duplicate values downwards starting from startCell
#     '''
#     try:
#         wb = xw.Book(outputFile)
#         sh = wb.sheets(sheetName)
#         symbols = sh.range(f'{symbolColumn}{str(symbolValueStartRow)}').expand('down')
#         for symbol in symbols:
            
#         sh.range('A2').options(pd.DataFrame, index=False, header=False).value = df
#         # sh.range('C1:F1').merge()
#         sh.range('A1').value = ['SYMBOL', 'STRIKE PRICE', 'ACTIVITY', 'ACTIVITY', 'ACTIVITY', 'ACTIVITY', 'SUPPORT 1', 'SUPPORT 2', 'RESISTANCE 1', 'RESISTANCE 2']
#         # sh.range('A1').expand('right').api.HorizontalAlignment = xw.constants.HAlign.xlHAlignCenter
#         sh.tables.add(sh.range('A1').expand('table'), table_style_name='TableStyleMedium13', has_headers=True, )
#         logging.info('Dashboard sheet updated')
#         wb.save()
#         logging.info(f'Workbook saved: {sh.name}')
#     except FileExistsError as e:
#         raise e
#     except Exception as e:
#         raise e
import logging

from sqlalchemy import false, true
from common.aws import AWS
from datetime import datetime as dt

class Market:
  def __init__(self, **kwargs) -> None:
    self.awsClient = AWS('ssm')
    for (key, value) in kwargs.items():
      if key == 'nseHolidayListSSMParameter':
        self.nseHolidayListParameter = value
  
  def isHolidayToday(self) -> bool:
    try:
      nseHolidayList = self.awsClient.getDetailsFromSSM(self.nseHolidayListParameter).split(',')
    except AttributeError:
      logging.error('it seems \'nseHolidayListSSMParameter\' was passed to initialize the market class')
    today = dt.now().strftime('%d-%b-%Y')
    logging.info('Checking if today is a holiday')
    return true if today in nseHolidayList else false

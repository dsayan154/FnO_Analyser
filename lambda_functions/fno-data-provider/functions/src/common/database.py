import sqlalchemy, pandas as pd

class DataBase:
  def __init__(self, dbDetails: dict) -> None:
    self.details: dict = {
      'host': dbDetails['host'],
      'port': dbDetails['port'],
      'user': dbDetails['username'],
      'password': dbDetails['password'],
      'db': dbDetails['dbname']
    }
    self.db = self.__getEngine()
  
  def __getEngine(self) -> sqlalchemy.engine.base.Engine:
    connString = 'mysql+mysqlconnector://'+self.details['user']+':'+self.details['password']+'@'+self.details['host']+':'+self.details['port']+'/'+self.details['db']
    return sqlalchemy.create_engine(connString)
  
  def overwriteTable(self, df: pd.DataFrame, tableName: str):
    with self.db.begin() as connection:
      df.to_sql(tableName, con=connection, if_exists='replace', index=False)
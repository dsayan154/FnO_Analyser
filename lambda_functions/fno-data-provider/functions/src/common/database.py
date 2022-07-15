import sqlalchemy, pandas as pd

class DataBase:
  def __init__(self, dbHost:str, dbPort: int, dbUsername: str, dbPassword: str, dbName: str) -> None:
    self.details: dict = {
      'host': dbHost,
      'port': dbPort,
      'user': dbUsername,
      'password': dbPassword,
      'db': dbName
    }
    self.db = self.__getEngine()
  
  def __getEngine(self) -> sqlalchemy.engine.base.Engine:
    connString = 'mysql+mysqlconnector://'+self.details['user']+':'+self.details['password']+'@'+self.details['host']+':'+self.details['port']+'/'+self.details['db']
    return sqlalchemy.create_engine(connString)
  
  def overwriteDataFrame(self, df: pd.DataFrame, tableName: str):
    with self.db.begin() as connection:
      df.to_sql(tableName, con=connection, if_exists='replace', index=False)
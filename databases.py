import urllib
from sqlalchemy import create_engine
import numpy as np
import os

class Database():
        def __init__(self, database=None):
            self.type = type
            if database:
                self.database = database

        def create_connection(self,server_address):
            quoted = urllib.parse.quote_plus(f"DRIVER={{SQL Server}};SERVER={server_address};DATABASE=")
            quoted = quoted + self.database
            engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
            print(engine)
            return engine


        def get_connection(self):
            if self.type == 'bi1':
                engine = self.create_connection(os.environ["bi1"])
            elif self.type == 'bi2':
                
                engine = self.create_connection(os.environ["bi2"])
            else:
                engine = None
            return engine


        def insert_dataframe(self,data,destination_table,insert_strategy):
            engine = self.get_connection()

            with engine.connect() as connection:
                data.drop_duplicates(keep='first', inplace=True)
                data = data.replace({np.nan: None})
                print(data)
                data.to_sql(destination_table, con=connection, index=False, if_exists=insert_strategy)
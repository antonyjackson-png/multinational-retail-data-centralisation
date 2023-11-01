import yaml
import sqlalchemy 

class DatabaseConnector:
    def __init__(self, yaml_file):
        self.yaml_file = yaml_file
        self.engine = None

    def read_db_creds(self):
        with open(self.yaml_file, 'r') as file:
            dict = yaml.safe_load(file)
            return dict
        
    def init_db_engine(self):
        dict = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = dict["RDS_HOST"]
        USER = dict["RDS_USER"]
        PASSWORD = dict["RDS_PASSWORD"]
        DATABASE = dict["RDS_DATABASE"]
        PORT = dict["RDS_PORT"]
        engine = sqlalchemy.create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        self.engine = engine

    def list_db_tables(self):
        if self.engine:
            inspector = sqlalchemy.inspect(self.engine)
            return inspector.get_table_names()
        
    def upload_to_db(self, dataframe, table_name):
        dict = self.read_db_creds()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = dict["SALES_DATA_HOST"]
        USER = dict["SALES_DATA_USER"]
        PASSWORD = dict["SALES_DATA_PASSWORD"]
        DATABASE = dict["SALES_DATA_DATABASE"]
        PORT = dict["SALES_DATA_PORT"]
        self.engine = sqlalchemy.create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        with self.engine.connect() as connection:
            dataframe.to_sql(table_name, self.engine, if_exists='replace')   
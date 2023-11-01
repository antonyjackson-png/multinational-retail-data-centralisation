import database_utils
import sqlalchemy
import pandas as pd
import tabula
import requests
import boto3


class DataExtractor:
    def __init__(self, databaseConnector, table_name):
        self.databaseConnector = databaseConnector
        self.table_name = table_name

    def read_rds_table(self):
        with self.databaseConnector.engine.connect() as connection:
            users = pd.read_sql_table(self.table_name, connection)
            return users
        
    def retrieve_pdf_data(self, pdf_path):
        dfs = tabula.read_pdf(pdf_path, pages="all")
        df = pd.concat(dfs)
        return df
    
    def list_number_of_stores(self, number_stores_endpoint, header_dictionary):
        response = requests.get(number_stores_endpoint, headers=header_dictionary)
        return response
    
    def retrieve_stores_data(self, store_endpoint, header_dictionary, number_of_stores):
        json_list = []
        for store_number in range(0, number_of_stores):
            individual_endpoint = store_endpoint + str(store_number)
            response = requests.get(individual_endpoint, headers=header_dictionary)
            json_list.append(response.json())
        df = pd.json_normalize(json_list).set_index('index')
        return df
    
    def extract_from_s3(self, s3_address):
        dict = self.databaseConnector.read_db_creds()
        
        s3 = boto3.client('s3',
                          aws_access_key_id = dict["AWS_ACCESS_KEY_ID"],
                          aws_secret_access_key = dict["AWS_SECRET_ACCESS_KEY"])
        bucket_and_filename = s3_address.replace("s3://", "").split("/")
        bucket_name = bucket_and_filename[0]
        filename = bucket_and_filename[1]
        s3.download_file(Filename=filename, Bucket=bucket_name, Key=filename)
        df = pd.read_csv(filename, index_col=0)
        return df
    
    def retrieve_sales_data(self, sales_endpoint):
        response = requests.get(sales_endpoint)
        data = response.json()
        df = pd.DataFrame(data)
        return df
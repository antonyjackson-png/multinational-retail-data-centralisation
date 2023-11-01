import database_utils
import data_extraction
import pandas as pd
import numpy as np


class DataCleaning:
    def __init__(self):
        self.dataframe = None

    def clean_user_data(self, dataframe):
        self.dataframe = dataframe
        # Set all "NULL" strings to np.nan
        self.dataframe = self.dataframe.replace("NULL", np.nan)
        
        # There are 21 users with "NULL" in every column
        #print(self.dataframe.isna().sum()) 

        # After dropping these users, there are no more "NULL" values
        self.dataframe.dropna(inplace=True)
        #print(self.dataframe.isna().sum())

        # There are 15 users with a 10-character alphanumeric string for the date_of_birth
        regex_expression = '^[a-zA-Z0-9]{10}$'
        self.dataframe.loc[self.dataframe['date_of_birth'].str.match(regex_expression), 'date_of_birth'] = np.nan
        #print(self.dataframe.isna().sum())

        # After dropping these users, there are no more "NULL" values
        self.dataframe.dropna(inplace=True)
        #print(self.dataframe.isna().sum())

        self.dataframe.first_name = self.dataframe.first_name.astype('string')
        self.dataframe.last_name = self.dataframe.last_name.astype('string')
        self.dataframe.date_of_birth = pd.to_datetime(self.dataframe.date_of_birth, format='mixed')
        self.dataframe.company = self.dataframe.company.astype('string')
        self.dataframe.email_address = self.dataframe.email_address.astype('string')
        self.dataframe.address = self.dataframe.address.astype('string')

        # There are 3 unique countries ['Germany', 'United Kingdom', 'United States']
        #print(self.dataframe.country.unique())
        self.dataframe.country = self.dataframe.country.astype('string')

        # There are 4 unique country codes ['DE', 'GB', 'US', 'GGB]
        #print(self.dataframe.country.unique())
        #print(self.dataframe.country_code.unique())

        # However, after examing the addresses of 'GGB', they are also 'United Kingdom'
        #print(self.dataframe[self.dataframe.country_code == 'GGB'])

        # Therefore, replace all 'GGB' country codes with 'GB'
        self.dataframe = self.dataframe.replace("GGB", "GB")
        # We now have three unique country codes: ['DE', 'GB', 'US']
        #print(self.dataframe.country_code.unique())
        self.dataframe.country_code = self.dataframe.country_code.astype('string')

        self.dataframe.phone_number = self.dataframe.phone_number.astype('string')
        self.dataframe.join_date = pd.to_datetime(self.dataframe.join_date, format='mixed')
        self.dataframe.user_uuid = self.dataframe.user_uuid.astype('string')

        # Finally, drop duplicates
        self.dataframe = self.dataframe.drop_duplicates()

        return self.dataframe
    
    def clean_card_data(self, dataframe):
        self.dataframe = dataframe
        
        # First convert card numbers to strings
        self.dataframe.card_number = self.dataframe.card_number.astype('string')
        # Remove '?' from card numbers that start with '?'
        self.dataframe.card_number = self.dataframe.card_number.str.replace('?', '')
        # Replace 'NULL' with '0'
        self.dataframe.card_number = self.dataframe.card_number.str.replace('NULL', '0')
        # Replace invalid 10-character hash expressions with '0'
        regex_expression = '^[a-zA-Z0-9]{10}$'
        self.dataframe.loc[self.dataframe['card_number'].str.match(regex_expression), 'card_number'] = '0'
        # Now convert strings to int64
        self.dataframe.card_number = self.dataframe.card_number.astype('int64')
        # Set '0's to np.nan
        self.dataframe.card_number = self.dataframe.card_number.replace(0, np.nan)
        # Drop 'na's
        self.dataframe.dropna(inplace=True)

        # Convert expiry_date to datetime objects
        self.dataframe.expiry_date = pd.to_datetime(self.dataframe.expiry_date, format='%m/%y')

        # Convert card_provider to strings
        self.dataframe.card_provider = self.dataframe.card_provider.astype('string')

        # Convert date_payment_confirmed to datetime objects
        self.dataframe.date_payment_confirmed = pd.to_datetime(self.dataframe.date_payment_confirmed, format='mixed')

        # Finally, drop duplicates
        self.dataframe = self.dataframe.drop_duplicates()

        return self.dataframe
    
    def called_clean_store_data(self, dataframe):
        self.dataframe = dataframe

        # Drop redundant 'lat' column
        self.dataframe.drop(['lat'], axis=1, inplace=True)

        self.dataframe.address = self.dataframe.address.astype('string')
        

        # Dealing with index 0 (web access) 
        self.dataframe.loc[0, 'longitude'] = '0.0'
        self.dataframe.loc[0, 'latitude'] = '0.0'

        # Replace 'NULL' with '99' (to drop rows)
        self.dataframe.longitude = self.dataframe.longitude.str.replace('NULL', '99')
        # Replace invalid 10-character hash expressions with '99'
        regex_expression = '^[a-zA-Z0-9]{10}$'
        self.dataframe.loc[self.dataframe['longitude'].str.match(regex_expression), 'longitude'] = '99'
        # Set '99's to np.nan
        self.dataframe.longitude[self.dataframe.longitude == "99"] = np.nan
        # Drop 'na's (except web access at index 0)
        self.dataframe.dropna(inplace=True)
        self.dataframe.longitude = self.dataframe.longitude.astype('float64')

        self.dataframe.locality = self.dataframe.locality.astype('string')
        self.dataframe.store_code = self.dataframe.store_code.astype('string')

        # Dealing with 'staff_numbers' typos
        self.dataframe.loc[31, 'staff_numbers'] = '78'
        self.dataframe.loc[179, 'staff_numbers'] = '30'
        self.dataframe.loc[248, 'staff_numbers'] = '80'
        self.dataframe.loc[341, 'staff_numbers'] = '97'
        self.dataframe.loc[375, 'staff_numbers'] = '39'
        self.dataframe.staff_numbers = self.dataframe.staff_numbers.astype('int')

        self.dataframe.opening_date = pd.to_datetime(self.dataframe.opening_date, format='mixed')

        self.dataframe.store_type = self.dataframe.store_type.astype('string')
        self.dataframe.latitude = self.dataframe.latitude.astype('float64')
        self.dataframe.country_code = self.dataframe.country_code.astype('string')

        # Dealing with duplicate 'continent' codes
        self.dataframe.continent = self.dataframe.continent.str.replace('eeEurope', 'Europe')
        self.dataframe.continent = self.dataframe.continent.str.replace('eeAmerica', 'America')
        self.dataframe.continent = self.dataframe.continent.astype('string')

        # Finally, drop duplicates
        self.dataframe = self.dataframe.drop_duplicates()

        return self.dataframe
    
    
    
    def convert_product_weights(self, dataframe):
        self.dataframe = dataframe
        # Drop 'NULL' to start
        self.dataframe.dropna(inplace=True)
        # Replace invalid 10-character hash expressions with '99'
        regex_expression = '^[a-zA-Z0-9]{10}$'
        self.dataframe.loc[self.dataframe['weight'].str.match(regex_expression), 'weight'] = '99'
        self.dataframe.weight[self.dataframe.weight == "99"] = np.nan
        # Drop NAs
        self.dataframe.dropna(inplace=True)

        self.dataframe.weight = self.dataframe.weight.astype('string')

        # For weights with value '12 x 100g', replace with '1.2'
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: '1.2' if x == "12 x 100g" else x)
        # For weights with value '8 x 150g', replace with '1.2'
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: '1.2' if x == "8 x 150g" else x)
        # For weights with value '6 x 412g', replace with '2.472'
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: '2.472' if x == "6 x 412g" else x)
        # For weights with value '6 x 400g', replace with '2.4'
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: '2.4' if x == "6 x 400g" else x)
        # For weights with value '8 x 85g', replace with '0.68'
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: '0.68' if x == "8 x 85g" else x)
        # For weights with value '40 x 100g', replace with '4.0'
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: '4.0' if x == "40 x 100g" else x)
        # For weights with value '12 x 85g', replace with '1.02'
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: '1.02' if x == "12 x 85g" else x)
        # For weights with value '3 x 2g', replace with '0.006'
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: '0.006' if x == "3 x 2g" else x)
        # For weights with value '3 x 90g', replace with '0.27'
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: '0.27' if x == "3 x 90g" else x)
        # For weights with value '16 x 10g', replace with '0.16'
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: '0.16' if x == "16 x 10g" else x)
        # For weights with value '3 x 132g', replace with '0.396'
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: '0.396' if x == "3 x 132g" else x)
        # For weights with value '5 x 145g', replace with '0.725'
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: '0.725' if x == "5 x 145g" else x)
        # For weights with value '2 x 200g', replace with '0.4'
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: '0.4' if x == "2 x 200g" else x)
        # For weights with value '4 x 400g', replace with '1.6'
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: '1.6' if x == "4 x 400g" else x)

        # For typo weight of '77g .', replace with '0.077'
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: '0.077' if x == "77g ." else x)

        # Convert the weight of '16oz' to '0.454', which uses the convertion factor 1/35.274
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: '0.454' if x == "16oz" else x)

        # For weights ending in 'kg', remove 'kg' 
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: x[:-2] if x[-2:] == "kg" else x)

        # For weights now ending in 'g', remove 'g', convert to float, divide by 1000, and convert back to str
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: str(float(x[:-1])/1000) if x[-1:] == "g" else x)

        # For weights now ending in 'ml', remove 'ml', convert to float, divide by 1000, and convert back to str
        self.dataframe['weight'] = self.dataframe['weight'].apply(lambda x: str(float(x[:-2])/1000) if x[-2:] == "ml" else x)
        
        return self.dataframe
    
    def clean_products_data(self, dataframe):
        self.dataframe = dataframe

        self.dataframe.product_name = self.dataframe.product_name.astype('string')
        self.dataframe.product_price = self.dataframe.product_price.astype('string')

        # 'product_price' column has a redundant single 'A£' character
        self.dataframe['product_price'] = self.dataframe['product_price'].apply(lambda x: x[1:]) 

        self.dataframe.product_price = self.dataframe.product_price.astype('float64')
        self.dataframe.weight = self.dataframe.weight.astype('float64')
        self.dataframe.category = self.dataframe.category.astype('string')
        self.dataframe.EAN = self.dataframe.EAN.astype('string')

        # If 'EAN' value starts with an apostrophe, remove the apostrophe
        self.dataframe['EAN'] = self.dataframe['EAN'].apply(lambda x: x[1:] if x[0] == "'" else x)
        self.dataframe.EAN = self.dataframe.EAN.astype('int64')

        self.dataframe.date_added = pd.to_datetime(self.dataframe.date_added, format='mixed')

        self.dataframe.uuid = self.dataframe.uuid.astype('string')
        self.dataframe.removed = self.dataframe.removed.astype('string')

        self.dataframe.product_code = self.dataframe.product_code.astype('string')

        # Finally, drop duplicates
        self.dataframe = self.dataframe.drop_duplicates()

        return self.dataframe
    
    def clean_orders_data(self, dataframe):
        self.dataframe = dataframe

        # Drop ['first_name', 'last_name', '1', columns]
        self.dataframe.drop(['first_name', 'last_name', '1'], axis=1, inplace=True)

        # Drop redundant 'level_0' column 
        self.dataframe.drop(['level_0'], axis=1, inplace=True)

        # Set index to index
        self.dataframe = self.dataframe.set_index('index')

        self.dataframe.date_uuid = self.dataframe.date_uuid.astype('string')
        self.dataframe.user_uuid = self.dataframe.user_uuid.astype('string')
        self.dataframe.store_code = self.dataframe.store_code.astype('string')
        self.dataframe.product_code = self.dataframe.product_code.astype('string')

        # Finally, drop duplicates
        self.dataframe = self.dataframe.drop_duplicates()

        return self.dataframe

    def clean_sales_details(self, dataframe):
        self.dataframe = dataframe

        # Replace 'NULL' with '99' (to drop rows)
        self.dataframe.timestamp = self.dataframe.timestamp.str.replace('NULL', '99')
        # Replace invalid 10-character hash expressions with '99'
        regex_expression = '^[a-zA-Z0-9]{10}$'
        self.dataframe.loc[self.dataframe['timestamp'].str.match(regex_expression), 'timestamp'] = '99'
        # Set '99's to np.nan
        self.dataframe.timestamp[self.dataframe.timestamp == "99"] = np.nan
        # Drop 'na's 
        self.dataframe.dropna(inplace=True)

        # Convert timestamp to datetime objects
        self.dataframe.timestamp = pd.to_datetime(self.dataframe.timestamp, format='%H:%M:%S')

        self.dataframe.month = self.dataframe.month.astype('int')
        self.dataframe.year = self.dataframe.year.astype('int')
        self.dataframe.day = self.dataframe.day.astype('int')

        self.dataframe.time_period = self.dataframe.time_period.astype('string')
        self.dataframe.date_uuid = self.dataframe.date_uuid.astype('string')

        # Finally, drop duplicates
        self.dataframe = self.dataframe.drop_duplicates()

        return self.dataframe




if __name__ == "__main__":
    databaseConnector = database_utils.DatabaseConnector("db_creds.yaml")
    databaseConnector.init_db_engine()
    dataCleaner = DataCleaning()

    
    table_name = 'legacy_users'
    dataExtractor = data_extraction.DataExtractor(databaseConnector, table_name)
    
    users_df = dataExtractor.read_rds_table().set_index('index')

    cleaned_users_df = dataCleaner.clean_user_data(users_df)
    databaseConnector.upload_to_db(cleaned_users_df, 'dim_users')
    

    card_details = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    cards_df = dataExtractor.retrieve_pdf_data(card_details)
    cleaned_cards_df = dataCleaner.clean_card_data(cards_df)
    databaseConnector.upload_to_db(cleaned_cards_df, 'dim_card_details')

    
    api_dict = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    response = dataExtractor.list_number_of_stores(number_stores_endpoint, api_dict)
    print(response.json()) # prints {'statusCode': 200, 'number_stores': 451}
    number_stores = 451
    store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
    stores_df = dataExtractor.retrieve_stores_data(store_endpoint, api_dict, number_stores)
    cleaned_stores_df = dataCleaner.called_clean_store_data(stores_df)
    databaseConnector.upload_to_db(cleaned_stores_df, 'dim_store_details')

    
    products_address = "s3://data-handling-public/products.csv"
    products_df = dataExtractor.extract_from_s3(products_address)
    converted_weights_df = dataCleaner.convert_product_weights(products_df)
    cleaned_products_df = dataCleaner.clean_products_data(converted_weights_df)
    databaseConnector.upload_to_db(cleaned_products_df, 'dim_products')

    
    table_name_2 = "orders_table"
    databaseConnector.init_db_engine()
    dataExtractor = data_extraction.DataExtractor(databaseConnector, table_name_2)
    orders_df = dataExtractor.read_rds_table()
    cleaned_orders_df = dataCleaner.clean_orders_data(orders_df)
    databaseConnector.upload_to_db(cleaned_orders_df, 'orders_table')
    
    sales_details = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    sales_details_df = dataExtractor.retrieve_sales_data(sales_details)
    cleaned_sales_details_df = dataCleaner.clean_sales_details(sales_details_df)
    databaseConnector.upload_to_db(cleaned_sales_details_df, 'dim_date_times')
    
    
    
    
    


    
    
    

    
    

    
    

    
    
    
    
   

    

    
import os
import pandas as pd
import sys
import shutil
from sklearn.model_selection import train_test_split
from src.logger import get_logger
from src.custom_exception import CustomException
from config.paths_config import *

downloads_folder = os.path.expanduser('./data')
target_folder = '/artifacts/raw'

csv_file_name = 'data.csv'  # Replace with your actual CSV file name

# Full path to the CSV file in Downloads
csv_file_path = os.path.join(downloads_folder, csv_file_name)

# Full path to the target location
target_csv_path = os.path.join(target_folder, csv_file_name)

logger = get_logger(__name__)

class DataIngestion:

    def __init__(self,raw_data_path , ingested_data_dir):
        self.raw_data_path = raw_data_path
        self.ingested_data_dir = ingested_data_dir
        logger.info("Data Ingestion has started")

    def create_ingested_data_dir(self):
        try:
            os.makedirs(self.ingested_data_dir , exist_ok=True)
            logger.info("Directory for Ingestion created")
        except Exception as e:
            raise CustomException("Error while creaing directory" , sys)

    def extract_csv(self,raw_data_path, ingested_data_dir,data_path):
    
        try:
        # Ensure the target directory exists
            #os.makedirs(target_folder, exist_ok=True)
        
        # Copy the CSV file to the target directory
            shutil.copy(raw_data_path, ingested_data_dir)
            print(f"CSV file successfully copied to {ingested_data_dir}")
        
        # Load and display the CSV data (optional)
            data = pd.read_csv(data_path)
            print(f"Data loaded successfully with shape: {data.shape}")
            print(data.head())
        
        except FileNotFoundError:
            print(f"Error: The file {csv_file_name} was not found in {downloads_folder}.")
        except Exception as e:
            print(f"An error occurred: {e}")   
    
        
    def split_data(self,data_path, train_path, test_path, test_size=0.2, random_state=42):
    
        try:
        # Load the raw data from the specified CSV file
            data = pd.read_csv(data_path)
            logger.info(f"Data loaded successfully from {data_path} with shape: {data.shape}")

        # Split the data
            train_data, test_data = train_test_split(data, test_size=test_size, random_state=random_state)
            logger.info("Data split successfully into training and testing sets")

        # Save the split data
            train_data.to_csv(train_path, index=False)
            test_data.to_csv(test_path, index=False)
            logger.info(f"Training data saved to {train_path}")
            logger.info(f"Testing data saved to {test_path}")

        except Exception as e:
           raise CustomException(f"Error while splitting data: {str(e)}", sys)
        
if __name__=="__main__":
    try:
        ingestion = DataIngestion(raw_data_path=RAW_DATA_PATH,ingested_data_dir=INGESTED_DATA_DIR)
        ingestion.create_ingested_data_dir()
        ingestion.extract_csv(raw_data_path=RAW_DATA_PATH,ingested_data_dir=INGESTED_DATA_DIR,data_path=DATA_PATH)
        ingestion.split_data(train_path=TRAIN_DATA_PATH,test_path=TEST_DATA_PATH,data_path=DATA_PATH)

    except CustomException as ce:
        logger.error(str(ce))

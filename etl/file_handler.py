from pathlib import Path
import logging
import urllib.request
import zipfile

from database_management import PostgreSQLConnector
from etl import DataVaultLoader


class FileHandler:
    def __init__(self, 
                 csv_dir='stack-exchange-data', 
                 zip_file='stack-exchange-data.zip', 
                 zip_url='https://cloudstorage.elearning.uni-oldenburg.de/s/ysmCtZCm3zDYb4r/download/stack-exchange-data.zip'):
        self.csv_dir = Path(csv_dir)
        self.zip_file = Path(zip_file)
        self.zip_url = zip_url
        
        # Initialize database connector
        self.db_connector = PostgreSQLConnector()
        
        # Setup logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def validate_paths(self):
        """Ensure that the CSV directory exists."""
        if not self.csv_dir.exists():
            self.logger.info(f"CSV directory {self.csv_dir} does not exist.")
            return False
        self.logger.info(f"CSV directory {self.csv_dir} exists.")
        return True

    def download_data(self):
        """Download and extract data if CSV directory does not exist."""
        if not self.validate_paths():
            self.logger.info("Downloading zip file from cloud...")
            try:
                urllib.request.urlretrieve(self.zip_url, self.zip_file)
                self.logger.info(f"Downloaded {self.zip_file}")
                
                with zipfile.ZipFile(self.zip_file, 'r') as zip_ref:
                    zip_ref.extractall('.')
                self.logger.info(f"Extracted {self.zip_file} contents to current directory.")
            except Exception as e:
                self.logger.error(f"Error during download or extraction: {e}")
                raise
        else:
            self.logger.info("CSV directory already exists. Skipping download.")

    def process_data(self):
        """Run the ETL process."""
        self.logger.info("Starting ETL process...")
        try:
            # Establish database connection
            conn = self.db_connector.connect_to_db() #This will get the connection

            # Pass the connection to the run_etl function
            from etl import DataVaultLoader #We import the SQL name
            data_loader = DataVaultLoader(self.db_connector)
            data_loader.run_etl(conn)

            self.logger.info("ETL process completed successfully.")
        except Exception as e:
            self.logger.error(f"ETL process failed: {e}", exc_info=True) #Capture the whole traceback
        finally:
            # Ensure the connection is closed if it was opened
            self.db_connector.log_message("Database connection closed.")
            if conn:
                conn.close()
                self.logger.info("Database connection closed.")

    def cleanup(self):
        """Clean up temporary files (e.g., the downloaded zip file)."""
        if self.zip_file.exists():
            try:
                self.zip_file.unlink()
                self.logger.info("Zip file cleaned up successfully.")
            except Exception as e:
                self.logger.error(f"Failed to remove zip file: {e}")

    def run_pipeline(self):
        """Execute the complete ETL pipeline: download data, create tables, process data, and cleanup."""
        try:
            # Create the tables in SQL before loading, this will ensure that all files are well formatted
            self.db_connector.create_and_verify_tables()
            self.download_data()
            self.process_data()
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}", exc_info=True) # Capture traceback at pipeline level

        finally:
            self.cleanup()


if __name__ == '__main__':
    handler = FileHandler()
    handler.run_pipeline()
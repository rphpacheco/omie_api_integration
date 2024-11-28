import pandas as pd
from src.config import Settings
from sqlalchemy import create_engine, text

from loguru import logger

settings = Settings()

class Database:
    """
    A class used to manage interactions with a PostgreSQL database, including creating connections,
    retrieving table columns, updating table structures, and saving data.
    """

    def __init__(self):
        """
        Initializes the Database instance, establishing a connection to the database.
        
        Attributes:
            engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine used to connect to the database.
            connection (sqlalchemy.engine.base.Connection): The active connection to the database.
        """
        self.engine = self.get_engine()
        self.connection = self.engine.connect()

    def get_engine(self):
        """
        Creates a SQLAlchemy engine to connect to the PostgreSQL database using settings.

        Returns:
            sqlalchemy.engine.base.Engine: An engine instance to connect with the PostgreSQL database.
        """
        connection_string = f"postgresql://{settings.DB_USERNANE}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
        engine = create_engine(connection_string)
        return engine

    def get_columns_of_db(self, table_name: str):
        """
        Retrieves the column names of a specified table from the database.

        Args:
            table_name (str): The name of the table for which column names are retrieved.

        Returns:
            list: A list of column names in the specified table.
        """
        query = text(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
        """)
        result = self.connection.execute(query)
        return [row[0] for row in result]

    def update_table_structure(self, table_name: str, df_columns):
        """
        Updates the structure of a specified table by adding missing columns from a DataFrame.

        Args:
            table_name (str): The name of the table to be updated.
            df_columns (list): A list of column names from the DataFrame.
        
        Notes:
            Only columns that are present in `df_columns` but missing in the table are added. 
            All added columns are of type TEXT.
        """
        existing_columns = self.get_columns_of_db(table_name)
        missing_columns = [col for col in df_columns if col not in existing_columns]
        print("Missing columns:", missing_columns)
        with self.connection as connection:
            try:
                for column in missing_columns:
                    transaction = connection.begin()
                    alter_query = text(f'ALTER TABLE {table_name} ADD COLUMN "{column}" TEXT;')
                    connection.execute(alter_query)
                    transaction.commit()
                
                logger.info(f"The table '{table_name}' has been successfully updated.")
            except Exception as e:
                logger.error(f"An error occurred while updating the table '{table_name}': {e}")

    def save_into_db(self, page: int, resource: str, content: dict):
        """
        Saves the given content into a database table, creating or appending data as needed.

        Args:
            page (int): The page number of the data; used to decide if the table is created or appended to.
            resource (str): A URL or path-like string used to derive the table name.
            content (dict): The data to be saved, structured as a dictionary.
        
        Notes:
            For the first page, the table is replaced with the new data. For subsequent pages, the table is updated with
            any new columns in the content before appending data.
        """
        table_name = resource.split("/")[-2]
        df = pd.json_normalize(content)

        try:
            if page == 1:
                df.to_sql(table_name, self.engine, if_exists='replace', index=False)
            else:
                self.update_table_structure(table_name, df.columns)
                df.to_sql(table_name, self.engine, if_exists='append', index=False)

            logger.info(f"Page {page} has been successfully saved in the table '{table_name}'.")
        except Exception as e:
            logger.error(f"An error occurred while saving the table '{table_name}': {e}")
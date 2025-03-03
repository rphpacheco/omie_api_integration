import pandas as pd
from src.config import Settings
from sqlalchemy import create_engine, text, event
from sqlalchemy.pool import QueuePool
from loguru import logger
import os

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
        """Creates a SQLAlchemy engine with connection pooling"""
        connection_string = f"postgresql://{settings.DB_USERNAME}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
        engine = create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_pre_ping=True
        )
        
        # Add event listeners for connection pooling
        @event.listens_for(engine, 'connect')
        def connect(dbapi_connection, connection_record):
            connection_record.info['pid'] = os.getpid()

        @event.listens_for(engine, 'checkout')
        def checkout(dbapi_connection, connection_record, connection_proxy):
            pid = os.getpid()
            if connection_record.info['pid'] != pid:
                connection_record.connection = connection_proxy.connection = None
                raise exc.DisconnectionError(
                    "Connection record belongs to pid %s, "
                    "attempting to check out in pid %s" %
                    (connection_record.info['pid'], pid)
                )
        
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

    def update_table_structure(self, table_name: str, new_columns):
        """Updates table structure to match new columns"""
        try:
            existing_columns = self.get_columns_of_db(table_name)
            for column in new_columns:
                if column not in existing_columns:
                    alter_query = text(f'ALTER TABLE {table_name} ADD COLUMN "{column}" TEXT')
                    self.connection.execute(alter_query)
            self.connection.commit()
        except Exception as e:
            logger.error(f"Error updating table structure for {table_name}: {e}")
            self.connection.rollback()

    def save_into_db(self, page: int, resource: str, content: dict, replace: bool = False):
        """
        Enhanced version of save_into_db that handles batch processing
        
        Args:
            page (int): The page number or batch start page
            resource (str): The resource identifier
            content (dict): The data to save
            replace (bool): Whether to replace the existing table (True for first batch)
        """
        table_name = resource.split("/")[-2]
        
        try:
            # Convert content to DataFrame
            if isinstance(content, dict):
                for key, value in content.items():
                    if isinstance(value, list) and value and isinstance(value[0], dict):
                        parent_keys = [k for k in content.keys() if k != key]
                        df = pd.json_normalize(
                            content,
                            record_path=key,
                            meta=parent_keys
                        )
            else:
                df = pd.json_normalize(content)

            # Optimize DataFrame for better memory usage
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype('string[pyarrow]')

            # Use SQLAlchemy engine directly for better performance
            if_exists = "replace" if replace else "append"
            df.to_sql(
                table_name,
                self.engine,
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=1000  # Process in chunks for better memory management
            )
            
            logger.success(f"{'Replaced' if replace else 'Appended'} data into table {table_name} starting from page {page}")
            
        except Exception as e:
            logger.error(f"Error saving data into table {table_name}: {e}")
            raise

    def select_from_table(self, table_name: str, distinct_column: str = None):
        try:
            if distinct_column:
                query = text(f'SELECT DISTINCT "{distinct_column}" FROM {table_name}')
                result = self.connection.execute(query)
                return [row[0] for row in result]
            else:
                query = text(f"SELECT * FROM {table_name}")
                result = self.connection.execute(query)
                return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Error selecting data from table {table_name}: {e}")
            return None

    def __del__(self):
        """Ensure proper cleanup of database connections"""
        try:
            self.connection.close()
            self.engine.dispose()
        except:
            pass
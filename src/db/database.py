import pandas as pd
from src.config import Settings
from sqlalchemy import create_engine, text, event, types
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
                from sqlalchemy import exc
                raise exc.DisconnectionError(
                    "Connection record belongs to pid %s, "
                    "attempting to check out in pid %s" %
                    (connection_record.info['pid'], pid)
                )
        
        return engine

    def execute_with_transaction(self, query, params=None):
        """Execute a query within a transaction"""
        with self.engine.begin() as connection:
            if params:
                return connection.execute(query, params)
            return connection.execute(query)

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
        result = self.execute_with_transaction(query)
        return [row[0] for row in result]

    def update_table_structure(self, table_name: str, new_columns):
        """Updates table structure to match new columns"""
        try:
            existing_columns = self.get_columns_of_db(table_name)
            with self.engine.begin() as connection:
                for column in new_columns:
                    if column not in existing_columns:
                        alter_query = text(f'ALTER TABLE {table_name} ADD COLUMN "{column}" TEXT')
                        connection.execute(alter_query)
        except Exception as e:
            logger.error(f"Error updating table structure for {table_name}: {e}")
            raise

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

            # Convert numeric columns to appropriate types
            # TODO: Add more numeric columns to the list or make it dynamic
            numeric_columns = [
                'nSaldo', 'nValorDocumento', 'nSaldoAnterior', 'nSaldoAtual',
                'nSaldoConciliado', 'nSaldoProvisorio', 'nLimiteCreditoTotal',
                'nSaldoDisponivel'
            ]
            
            for col in df.columns:
                if col in numeric_columns and col in df.columns:
                    df[col] = pd.to_numeric(df[col].replace(['', None], '0'), errors='coerce')
                elif df[col].dtype == 'object':
                    df[col] = df[col].astype(str)

            # Create table with correct column types if it doesn't exist
            if replace or not self.table_exists(table_name):
                with self.engine.begin() as connection:
                    # Drop table if replacing
                    if replace and self.table_exists(table_name):
                        connection.execute(text(f'DROP TABLE IF EXISTS {table_name}'))

                    # Create column definitions
                    columns = []
                    for col in df.columns:
                        if col in numeric_columns:
                            columns.append(f'"{col}" NUMERIC(15,2)')
                        else:
                            columns.append(f'"{col}" TEXT')
                    
                    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
                    connection.execute(text(create_table_sql))

            # Define SQLAlchemy types for columns
            dtype = {}
            for col in df.columns:
                if col in numeric_columns:
                    dtype[col] = types.Numeric(15, 2)
                else:
                    dtype[col] = types.Text()

            # Use SQLAlchemy engine directly for better performance
            df.to_sql(
                table_name,
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000,
                dtype=dtype
            )
            
            logger.success(f"{'Replaced' if replace else 'Appended'} data into table {table_name} starting from page {page}")
            
        except Exception as e:
            logger.error(f"Error saving data into table {table_name}: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database"""
        query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = :table_name
            )
        """)
        result = self.execute_with_transaction(query, {"table_name": table_name}).scalar()
        return bool(result)

    def select_from_table(self, table_name: str, distinct_column: str = None):
        try:
            if distinct_column:
                query = text(f'SELECT DISTINCT "{distinct_column}" FROM {table_name}')
                result = self.execute_with_transaction(query)
                return [row[0] for row in result]
            else:
                query = text(f"SELECT * FROM {table_name}")
                result = self.execute_with_transaction(query)
                return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Error selecting data from table {table_name}: {e}")
            return None

    def __del__(self):
        """Ensure proper cleanup of database connections"""
        try:
            if hasattr(self, 'connection'):
                self.connection.close()
            if hasattr(self, 'engine'):
                self.engine.dispose()
        except:
            pass
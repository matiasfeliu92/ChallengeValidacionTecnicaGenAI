import json
import platform
from typing import List

import pandas as pd
from sqlalchemy import JSON
from src.config.settings import Settings
from src.config.logger import LoggerConfig
from src.config.db import ManageDB

class LoadData:
    def __init__(self):
        self.db_config = ManageDB()
        self.settings = Settings()
        self.logger = LoggerConfig.get_logger(self.__class__.__name__)
        self.conn_string_default_DB = self.settings.POSTGRES_CONNECTION_STRING_DEFAULT_DB if platform.system() == "Windows" else self.settings.POSTGRES_CONNECTION_STRING_DOCKER_DEFAULT_DB
        self.conn_string_for_cursor_default_DB = self.settings.POSTGRES_CURSOR_CONNECTION_STRING_DEFAULT_DB if platform.system() == "Windows" else self.settings.POSTGRES_CURSOR_CONNECTION_STRING_DOCKER_DEFAULT_DB
        self.engine = None
        self.conn_string_new_DB = self.settings.POSTGRES_CONNECTION_STRING_NEW_DB if platform.system() == "Windows" else self.settings.POSTGRES_CONNECTION_STRING_DOCKER_NEW_DB
        self.conn_string_for_cursor_new_DB = self.settings.POSTGRES_CURSOR_CONNECTION_STRING_NEW_DB if platform.system() == "Windows" else self.settings.POSTGRES_CURSOR_CONNECTION_STRING_DOCKER_NEW_DB

    def load(self, __data__: List[dict]):
        self.logger.info(f"SYSTEM PLATFORM: {platform.system()}")
        self.logger.info("Starting database connection and setup...")
        self.engine = self.db_config.create_engine(self.conn_string_default_DB)
        conn = self.db_config.create_connection(self.conn_string_for_cursor_default_DB)
        self.db_config.create_database(conn, self.settings.POSTGRES_DB_NAME_USE)
        conn.close()
        self.engine = self.db_config.create_engine(self.conn_string_new_DB)
        self.logger.info("Database and schema setup completed.")
        self.logger.info("Starting data load into database...")
        table_name = "raw_logs"
        df = pd.DataFrame({"data": __data__})
        df.to_sql(
            name=table_name,
            con=self.engine,
            if_exists="replace",
            index=False,
            dtype={"data": JSON}
        )
        self.logger.info(f"Data successfully loaded into table: {table_name}")
        self.logger.info("Data load process completed.")
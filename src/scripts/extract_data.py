import json
from typing import List

import pandas as pd
from src.config.settings import Settings
from src.config.logger import LoggerConfig
from src.utils.parse_json import ParseJson


class ExtractData:
    def __init__(self):
        self.settings = Settings()
        self.base_dir = self.settings.BASE_DIR
        self.logger = LoggerConfig.get_logger(self.__class__.__name__)
        self.parser = ParseJson()
        self.validated_json: List[dict] = []

    def extract(self):
        json_file_path = self.settings.get_dir("data", "raw", "mock_event_logs.json")
        self.logger.info(f"Data directory path: {json_file_path}")
        with open(json_file_path, "r", encoding="utf-8") as archivo:
            self.extracted_json = json.loads(archivo.read())
            self.logger.info("JSON data successfully read from file.")
            self.logger.debug(f"JSON Data: {self.extracted_json}")
        self.logger.info(f"Total records extracted: {len(self.extracted_json)}")
        for _, elem in enumerate(self.extracted_json):
            try:
                validated_record = self.parser.validate_json(_, elem)
                self.logger.info(f"Record {_} successfully validated and added to the list.")
                self.logger.debug(f"Validated Record {_}: {validated_record.dict()}")
                self.validated_json.append(validated_record.dict())
            except Exception as e:
                self.logger.error(f"Error validating record {_}: {e}") 
            self.logger.info(f"")
            self.logger.info(f"")
            self.logger.info(f"")
        return self.validated_json

import pandas as pd
from pydantic import ValidationError

from src.schemas.parsed_json import ParsedJson
from src.config.logger import LoggerConfig

class ParseJson:
    def __init__(self):
        self.logger = LoggerConfig.get_logger(self.__class__.__name__)

    def validate_json(self, __event__, __json__):
        self.logger.info("Validating JSON data against schema.")
        self.logger.debug(f"Input JSON: {__json__}")
        try:
            json_validated = ParsedJson(**__json__)
            self.logger.info("JSON data successfully validated.") 
            self.logger.debug(f"Validated JSON: {json_validated.dict()}") 
            return json_validated
        except ValidationError as e: 
            self.logger.error(f"Validation error: {e}") 
            raise
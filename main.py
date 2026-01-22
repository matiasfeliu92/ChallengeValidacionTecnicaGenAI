from src.scripts.load_data import LoadData
from src.scripts.extract_data import ExtractData

if __name__ == "__main__":
    extract_data = ExtractData()
    data_extracted = extract_data.extract()
    load_data = LoadData()
    load_data.load(data_extracted)
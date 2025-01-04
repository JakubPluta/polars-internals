import pathlib

ROOT_DIR_PATH = pathlib.Path(__file__).resolve().parent.parent
DATA_DIR_PATH = ROOT_DIR_PATH / "data"
COOKBOOK_DATA_DIR = DATA_DIR_PATH / "cookbook" / "data"
NYC_YELLOW_TAXI_DIR = DATA_DIR_PATH / "nycyellowtaxi"

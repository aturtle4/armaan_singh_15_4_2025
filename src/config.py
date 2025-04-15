import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_DIR = os.path.join(BASE_DIR, "CsvFiles")
DB_PATH = os.path.join(BASE_DIR, "Database", "database.db")
REPORT_DIR = os.path.join(BASE_DIR, "Reports")
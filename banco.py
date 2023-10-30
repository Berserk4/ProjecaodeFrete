import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv(r'D:\\PRojetos\\Codes\\GFL\Sinclog(RPA)\Docs\\Pa\\config_banco.env')

class DatabaseConnection:
    def __init__(self):
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.host = os.getenv('DB_HOST')
        self.database = os.getenv('DB_DATABASE')
        self.port = os.getenv('DB_PORT')
        self.connection = None
        self.cursor = None

    def __enter__(self):
        self.connection = mysql.connector.connect(
            user=self.user, 
            password=self.password, 
            host=self.host, 
            database=self.database,
            port=int(self.port)
        )
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.connection.close()
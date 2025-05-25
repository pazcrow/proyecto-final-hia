import sqlite3
import pandas as pd
import random 
from faker import Faker
from faker import Faker

class database:

    def __init__(self):
        self.path = 'base/HRD_Dataset.db'
        self.conn = ''
        self.cursor = ''
        self.gender_list = ["M", "F"]
        self.marital_list = ["Soltero", "Casado", "Diviorciado","Viudo"]
        self.fake = Faker('es_ES')
        self.db_path = "base/hdr.db"


    def connect_db(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            print("Conexion exitosa")
            return True
        except Exception as e:
            print(e)
            return False

    def create_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS hrd (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            EmpID INTEGER NOT NULL,
            University TEXT NOT NULL,
            State TEXT NOT NULL,
            GenderID INTEGER NOT NULL,
            MaritalStatusID INTEGER NOT NULL
        );
        """)

    
    def insert_register(self):
        df = pd.read_csv("data/last_dataset.csv")

        # Ensure required columns are present
        required_columns = ['EmpID', 'University', 'State', 'GenderID', 'MaritalStatusID']
        if not all(col in df.columns for col in required_columns):
            raise ValueError(f"Missing one or more required columns: {required_columns}")

        list_person = df[required_columns].values.tolist()

        self.cursor.executemany("""
            INSERT INTO hrd (EmpID, University, State, GenderID, MaritalStatusID)
            VALUES (?, ?, ?, ?, ?);
        """, list_person)

        self.conn.commit()
        self.conn.close()

    def random_gender(self):
        return self.gender_list[random.randint(0, len(self.gender_list)-1)]
    
    def random_marital_status(self):
        return self.marital_list[random.randint(0, len(self.marital_list)-1)]
    
    def random_age(self):
        return random.randint(18, 65)


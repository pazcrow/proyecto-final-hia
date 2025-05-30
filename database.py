import sqlite3
import pandas as pd


class database:

    def __init__(self):
        self.path = 'base/HRD_Dataset.db'
        self.conn = ''
        self.cursor = ''
        self.db_path = "base/hdr.db"


    def connect_db(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            print("INFO: Conexion exitosa a base de datos")
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


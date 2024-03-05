import mysql.connector
import re
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("mysql+pymysql://root:comsc@localhost/testdb?charset=utf8mb4")

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="comsc",
    database="testdb",
    charset='utf8mb4'
)

cur=mydb.cursor()

def create_raw_data_tbl():
    try:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            Title LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
            Start_date DATE,
            Mean DOUBLE,
            Rank INT,
            Popularity INT,
            Num_eps INT,
            Rating VARCHAR(10),
            Studio VARCHAR(255),
            Genres LONGTEXT,
            Synopsis LONGTEXT
        )
        CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        mydb.commit()
        cur.close()
    
    except Exception as e:
        print("Error: ", e)
    finally:
        mydb.close()

# For inserting all columns into raw_data had to remove non asc2 chars (title and synopsis are main culprits)
def clean_text(text):
    text = text.encode('ascii', 'ignore').decode('ascii')
    text = re.sub(r'[^\x00-\x7F]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text

try:
    df = pd.read_csv('anime_data.csv', encoding='utf-8', 
                     names=['ID', 'Title', 'Start_Date','Mean', 'Rank', 'Popularity', 'Num_eps', 'Rating', 'Studio', 'Genres', 'Synopsis'])
    for column in ['Title', 'Studio', 'Genres', 'Synopsis']:
        df[column] = df[column].astype(str).apply(clean_text) 

except Exception as error:
    print("Error reading CSV file:", error)

try:
    df.to_sql('raw_data', engine, if_exists='replace', index=False)

except Exception as e:
    print("Error inserting data:", e)
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
import re
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from sqlalchemy.sql import text

# Words to filter synopses by.
filter_words = [
    "1.",
    "#1",
    "1:",
    "note:",
    "source:",
    "credit:",
    "special 1",
    "Episode 1:",
    "Episode 01",
    "no synopsis has been added",
    "no synopsis yet",
    "based on a game",
    "DVD special",
    "TV special",
    "special episode",
    "special episodes",
    "original animation",
    "OVA",
    "based on a manga",
    "based on a novel",
    "based on a light novel",
    "bonus episodes",
    "bonus episode",
    "spin-off",
    "adaptation of",
    "alternate retelling",
    "alternate version",
    "recap",
    "remake of",
    "summary",
    "compilation",
    "anthology",
    "music video",
    "promotional video",
    "pilot episode",
    "preview",
    "teaser",
    "crossover",
    "chibi style",
    "fan-made",
    "CG animation",
    "stop-motion animation",
    "produced by",
    "collaboration",
    "original work",
    "photo session",
    "motion comic",
    "television program",
    "web anime",
    "ONA",
    "animated commercial",
    "animated PV",
    "short film",
    "short episode",
    "short episodes",
    "mini episode",
    "mini episodes",
    "film festival",
    "crowdfunded",
    "fan service",
    "filler episode",
    "filler episodes",
    "director's cut",
    "uncut version",
    "extended version",
    "limited series",
    "experimental film",
    "educational film",
    "silent film",
    "lost episode",
    "lost episodes",
    "lost film",
    "restored version",
    "remastered version",
    "directorial debut",
    "unfinished film",
    "post-credits scene",
    "mid-credits scene",
    "after-credits scene",
    "originally created",
    "prologue",
    "epilogue",
    "concept film",
    "concept video",
    "video diary",
    "visual essay",
    "art project",
    "fan project",
    "community project",
    "collage",
    "mashup",
    "parody",
    "satire",
    "fan fiction",
    "fanfic",
    "tribute",
    "homage",
    "doujinshi",
    "fan dub",
    "fandub",
    "non-canon",
    "noncanonical",
    "unofficial",
    "bootleg",
    "imitation",
    "spoof",
    "mockumentary",
    "fan edit",
    "fan-edited",
    "amateur film",
    "amateur production",
    "fan creation",
    "fan-created",
    "fan-made version",
    "fan-made film",
    "fan-made series",
    "unauthorized",
    "unauthorized version",
    "unauthorized adaptation",
    "unlicensed",
    "unlicensed version",
    "unlicensed adaptation",
    "unlicensed production",
    "bootleg version",
    "bootleg adaptation",
    "bootleg production",
    "pirated version",
    "pirated adaptation",
    "pirated production",
    "counterfeit version",
    "counterfeit adaptation",
    "counterfeit production",
    "fake version",
    "fake adaptation",
    "fake production",
    "imitation version",
    "imitation adaptation",
    "imitation production",
    "replica version",
    "replica adaptation",
    "replica production",
    "replication version",
    "replication adaptation",
    "replication production",
    "duplicate version",
    "duplicate adaptation",
    "duplicate production",
    "copycat version",
    "copycat adaptation",
    "copycat production",
    "knockoff version",
    "knockoff adaptation",
    "knockoff production",
    "ripoff version",
    "ripoff adaptation",
    "ripoff production",
    "clone version",
    "clone adaptation",
    "clone production",
    "forgery version",
    "forgery adaptation",
    "forgery production",
    "fraudulent version",
    "fraudulent adaptation",
    "fraudulent production",
    "counterfeited version",
    "counterfeited adaptation",
    "counterfeited production",
    "falsified version",
    "falsified adaptation",
    "falsified production",
    "fabricated version",
    "fabricated adaptation",
    "fabricated production",
    "deceptive version",
    "deceptive adaptation",
    "deceptive production",
    "misleading version",
    "misleading adaptation"
]

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="comsc",
    database="testdb",
    charset='utf8mb4'
)

cur=mydb.cursor()

engine = create_engine("mysql+pymysql://root:comsc@localhost/testdb?charset=utf8mb4")

# Function for creating mysql table called processed_synopsis. Called in first_synopsis_fitlering()
def create_processed_synopsis_tbl():

    try: 
        cur.execute("""
        CREATE TABLE IF NOT EXISTS processed_synopsis ( 
            ID INT AUTO_INCREMENT PRIMARY KEY,
            Original_ID INT,
            Filtered_Synopsis LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
            Tokenised_Synopsis LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
            Lemmatised_Synopsis LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
            Genres VARCHAR(255)
        )            
        ChARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        mydb.commit()
    except Exception as e:
        print("Error: ", e)
    finally:
        mydb.close()

# Check for the number of synposes that contain words we want to filter. No filtering done here.
def is_relevant_synopsis():
    try:
        query = "SELECT Synopsis FROM raw_data"
        synopsis = pd.read_sql(query, engine)
        counter = 0
    
        for index, row in synopsis.iterrows():
            synopsis_text = row['Synopsis']
            
            if any(words in synopsis_text for words in filter_words):
                counter +=1
                print(synopsis_text)
                print(counter)
    except Exception as e:
        print("Error: ", e)
    finally:
        mydb.close()


# Function to filter words
def contains_stop_word(text):
    for stop_word in filter_words:
        if stop_word in text.lower():
            return True
    return False

# Filter out the synopses that containt the unwanted words. Create the processed_synopsis table and validate
def first_synopsis_filtering():
        try:
            query = "SELECT id, Synopsis, Genres FROM raw_data"
            df = pd.read_sql(query, engine)
            df = df[~df['Synopsis'].apply(contains_stop_word)]
            df = df[df['Synopsis'].str.split().str.len() >= 15]
            df = df.dropna(subset=['Synopsis'])

            # Rename df headers to that in processed_synopsis table
            df = df.rename(columns={'id' : 'Original_ID', 'Synopsis' : 'Filtered_Synopsis'  })
            
            create_processed_synopsis_tbl() # Function to create the new synopsis table.
            df['Tokenised_Synopsis'] = None
            df['Lemmatised_Synopsis'] = None
            df.to_sql('processed_synopsis', engine, if_exists='replace', index=False)

            query_to_check = "SELECT * FROM processed_synopsis LIMIT 5"
            data = pd.read_sql(query_to_check, engine)
            print(data)

        except Exception as e:
            print("Error: ", e)
        finally:
            engine.dispose()

# Tokenisation of synopses
def synopsis_tokenisation():
    try:
        query = "SELECT Original_ID, Filtered_Synopsis FROM processed_synopsis"
        df = pd.read_sql(query, engine)
        
        stop_words = set(stopwords.words('english'))
        lemmatiser = WordNetLemmatizer()
        
        def clean_text(text):
            text = text.replace("é", "e")
            text = text.replace("è", "e") 
            text = re.sub(r"[^a-zA-Z'\s]", '', text)
            text = re.sub(r"'s\b", "", text)
            text = re.sub(r"\s'\s", " ", text)
            text = re.sub(r"'\s", " ", text)
            text = re.sub(r"\s'", " ", text)


            return text.lower()

        def tokenise_and_filter(text):
            tokens = word_tokenize(text.lower())
            return [word for word in tokens if word not in stop_words and len(word) > 1]
        
        def lemmatise_tokens(tokens):
            return [lemmatiser.lemmatize(word) for word in tokens]
       
        def list_to_string(lst):
            return ', '.join(lst)
        
        # Removing special characters, punctuation, numbers from Filtered_Synopsis
        df['Filtered_Synopsis'] = df['Filtered_Synopsis'].apply(clean_text)
        df['Tokenised_Synopsis'] = df['Filtered_Synopsis'].apply(tokenise_and_filter)
        df['Lemmatised_Synopsis'] = df['Tokenised_Synopsis'].apply(lemmatise_tokens)

        
        # Debugging: Check the results of tokenization and lemmatization
        print("Sample tokenization and lemmatization:")
        print(df[['Tokenised_Synopsis', 'Lemmatised_Synopsis']].head())

        print("Data before updating:")
        print(df[['Original_ID', 'Tokenised_Synopsis', 'Lemmatised_Synopsis']].head())
        
        # Updating table with the tokenised and lemmatised synopses
        with engine.connect() as conn:
            update_statement = text('''
                UPDATE processed_synopsis
                SET Tokenised_Synopsis = :tokenised, Lemmatised_Synopsis = :lemmatised
                WHERE Original_ID = :original_id
            ''')
            for index, row in df.iterrows():
                tokenised = list_to_string(row['Tokenised_Synopsis'])
                lemmatised = list_to_string(row['Lemmatised_Synopsis'])
                original_id = float(row['Original_ID'])  

                if tokenised is None or tokenised == "":
                    print(f"Empty tokenised for ID {original_id}")
                    continue  
                if lemmatised is None or lemmatised == "":
                    print(f"Empty lemmatised for ID {original_id}")
                    continue  
                
                params = {
                    'tokenised': tokenised,
                    'lemmatised': lemmatised,
                    'original_id': original_id
                }
                
                try:
                    result = conn.execute(update_statement, params)
                    conn.commit()
                    print(f"Updated ID {original_id}, affected rows: {result.rowcount}")
                except Exception as e:
                    print(f"Error updating ID {original_id}: {e}")               
                
        # Check if succesful.
        query = "SELECT Original_ID, Tokenised_Synopsis, Lemmatised_Synopsis FROM processed_synopsis LIMIT 15"
        updated_df = pd.read_sql(query, engine)
        print("Updated DataFrame:")
        print(updated_df)    

    except Exception as e:
        print("Error: ", e)
    
    finally:
        engine.dispose()

# Check for number of rows 
def row_count(i):
    try:
        row_count_query = "SELECT COUNT(*) FROM {i}"
        row_count = pd.read_sql(row_count_query, engine)
        print(row_count)
        
    except Exception as e:
        print("Error: ", e)
    finally: 
        mydb.close()







    
          

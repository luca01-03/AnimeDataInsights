import requests
import time           
import csv
import re
import time


def get_anime_data(client_id, anime_id):
    url =  f'https://api.myanimelist.net/v2/anime/{anime_id}?fields=title,start_date,end_date,mean,rank,popularity,num_list_users,num_scoring_users,genres,num_episodes,rating,studios,statistics,synopsis'

    headers = {
        'X-MAL-CLIENT-ID' : client_id
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(response.text)
        return None

client_id =  '6114d00ca681b7701d1e15fe11a4987e'


# Write to the txt file the anime_id
def update_last_fetched_id(anime_id):
    with open("id.txt", "w",  encoding='UTF-8') as file: 
        file.write(str(anime_id))

# Open the txt file and retrieve the anime_id
def retrieve_last_fetched_id():
    try:
        with open("id.txt", "r") as file:
            return int(file.read().strip())
    except FileNotFoundError:
        return 0


def data_to_csv(data):
    try:
        field_names = ['id', 'title', 'start_date', 'mean', 'rank', 'popularity', 'num_episodes', 'rating', 'studio', 'genres', 'synopsis']
        with open("anime_data.csv", "a" ,encoding='utf-8') as file:
         writer = csv.DictWriter(file, fieldnames=field_names, quoting=csv.QUOTE_ALL)
         writer.writerow(data)
            
    except FileNotFoundError:
        print("File not found.")
   


# Parse the API data to a list
def parse_data(data):
    try: 
        genres_list = data['genres']
        genres = ', '.join(genre['name'] for genre in genres_list)
        
        studio_name = "unknown"
        if 'studios' in data and len(data['studios']) > 0:
            studio_name = data['studios'][0].get('name', 'unknown')

        synopsis_dirty = data['synopsis']
        synopsis = re.sub(r"\(Source: [^\)]+\)", "", synopsis_dirty).strip()
        synopsis = re.sub(r'\[Written by MAL Rewrite\]', '', synopsis).strip()

        parsed_data = {
            "id": str(data.get('id', 'N/A')),
            "title": data.get('title', 'N/A'),
            "start_date": data.get('start_date', 'N/A'),
            "mean": str(data.get('mean', 'N/A')),
            "rank": str(data.get('rank', 'N/A')),
            "popularity": str(data.get('popularity', 'N/A')),
            "num_episodes": str(data.get('num_episodes', 'N/A')),
            "rating": data.get('rating', 'N/A'),
            "studio": studio_name,
            "genres": genres,
            "synopsis": synopsis
        }
        return parsed_data

    except Exception as e:
         
        print(f"Error in parse_data: {e}")
        return {key: 'N/A' for key in ['id', 'title', 'start_date', 'mean', 'rank', 'popularity', 'num_episodes', 'rating', 'studio', 'genres', 'synopsis']}
       
last_fetched_id = retrieve_last_fetched_id()

# Adjust number of calls done. API call limit is 100, per time is not known, so experimentation was done.
calls_made = 0
max_calls = 80

for anime_id in range(last_fetched_id + 1, last_fetched_id + 101):
    if calls_made >= max_calls:
        print("API call limit...Pausing for a few minutes.")
        time.sleep(240)
        calls_made = 0

    anime_data = get_anime_data(client_id, anime_id) 
    
    if anime_data: 
        update_last_fetched_id(anime_id)
        parsed_data = parse_data(anime_data)
        data_to_csv(parsed_data)
        calls_made += 1
    else:
        None
    
    
    
    
    
    
    





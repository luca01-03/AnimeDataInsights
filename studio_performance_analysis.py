from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score

engine = create_engine("mysql+pymysql://root:comsc@localhost/testdb?charset=utf8mb4")

def df(): 
    try:
        df = pd.read_csv('anime_data.csv', encoding='utf-8', 
                        names=['ID', 'Title', 'Start_Date','Mean', 'Rank', 'Popularity', 'Num_eps', 'Rating', 'Studio', 'Genres', 'Synopsis'])
        df.drop(['Synopsis', 'Title'], axis='columns', inplace=True, errors='raise') # Remove synopsis and title columns as they are not needed.
        df = df.dropna(subset=['Studio']) 
        df = df[df['Studio'] != 'unknown'] 
        return df
    except Exception as e: 
        print("An error: ", e)  


# Create mysql table called perf_an
def df_to_sql(df):
    try:
        df.to_sql('perf_an', engine, if_exists='replace', index=False)
    except Exception as e:
        print("Error inserting data:", e)

# Bar and whisker plots
def studio_eda(df):
    studio_counts = df['Studio'].value_counts()
    mean_ratings = df.groupby('Studio')['Mean'].mean()
    mean_popularity = df.groupby('Studio')['Popularity'].mean() 
    
    # Bar chart showing top 15 studios by anime count
    plt.figure(figsize=(10,6))
    bars = plt.bar(studio_counts.head(15).index, studio_counts.head(15).values)
    
    for bar in bars: 
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2.0, yval,  int(yval), va='bottom', ha='center')
   
    plt.title('Top 15 Studios by Anime Made')
    plt.ylabel('Number of Animes')
    plt.xlabel('Studio')
    plt.xticks(rotation=45, ha='right')

    # Box plot for rating distribution of top 10 studios
    top_studios = studio_counts.head(10).index
    top_studios_data = df[df['Studio'].isin(top_studios)]
    
    plt.figure(figsize=(12, 6))
    sns.boxplot(x='Studio', y='Mean', data=top_studios_data)
    plt.xticks(rotation=45, ha='right')
    plt.title('Rating Distributions for Top 10 Studios (By anime made)')
    plt.ylabel('Rating')
    plt.show()

# Heatmaps
def analyse_genre_distribution(df):
    df['Genres'] = df['Genres'].astype(str)
    expanded_genres = df.assign(Genres=df['Genres'].str.split(', ')).explode('Genres')

    genre_studio_count = expanded_genres.groupby(['Genres', 'Studio']).size().unstack(fill_value=0)
    
    top_studios = df['Studio'].value_counts().head(15).index
    genre_studio_top = genre_studio_count[top_studios]
    
    # same as top_studios but contains all studios for when creating tableau heatmap.
    top_studios_all = df['Studio'].value_counts().index 
    all_studios_heat = genre_studio_count[top_studios_all]

    # Export for tableau, contains all studios for heatmap. 
    all_studios_heat.to_csv('All_Studios_heatmap.csv', index='True')
    
    # The genre count across all studios
    genre_count = genre_studio_top.sum('columns')
    
    # The genre count for top 15 studios
    studio_genre_count = genre_studio_top.sum('rows')
    
    # Removing genres that have appear in less than 70 animes produced
    genres_to_keep = genre_count[genre_count>=70].index 
    genre_studio_top_filtered = genre_studio_top.loc[genres_to_keep]

    # Export for tableau. Only contains the top 15 studios. 
    genre_studio_top_filtered.to_csv('heatmap_data.csv', index='True') 
    
    # Heatmap showing the distribution of genres across the 15 studios that have produced the most anime.
    plt.figure(figsize=(12, 6))
    sns.heatmap(genre_studio_top_filtered, cmap='YlGnBu', annot=False)
    plt.title('Genre Distribution Across Top 15 Studios')
    plt.ylabel('Genres')
    plt.xlabel('Studio')
    plt.xticks(rotation=45, fontsize=9, ha='right')
    plt.yticks(fontsize=9)
    plt.show()

# Time series analysis
def TSA(df):
    
    df['Start_Date'] = pd.to_datetime(df['Start_Date'], errors='coerce')
    df['Year'] = df['Start_Date'].dt.year

    top_studios = df['Studio'].value_counts().head(4).index.tolist()
    top_studios_df = df[df['Studio'].isin(top_studios)]
    
    studio_yearly_ratings = top_studios_df.groupby(['Studio', 'Year'])['Mean'].mean().reset_index()

    # Line graph showing the average annual ratings
    plt.figure(figsize=(14, 7))
    for studio in top_studios:
        studio_data = studio_yearly_ratings[studio_yearly_ratings['Studio'] == studio]
        studio_data.set_index('Year', inplace=True)
        studio_data['Mean'].rolling(window=3, center=True).mean().plot(label=studio)

    plt.legend()
    plt.xlabel('Year')
    plt.ylabel('Mean Rating')
    plt.title('Mean Ratings of Top 5 Studios Over Years (Smoothed with 3-Year Moving Average)')
    plt.grid(True)
    plt.show()
    
# 0.1 correlation between number of episodes and anime rating
def corr_eps_to_rating(df):
    print(corr = df['Num_eps'].corr(df['Mean']))
    

# Histogram 
def anime_per_year_histogram(df):
    
    df['Start_Date'] = pd.to_datetime(df['Start_Date'], errors='coerce')
    df['Year'] = df['Start_Date'].dt.year

    df = df.dropna(subset=['Year'])
    df['Year'] = df['Year'].astype(int) 

    # Histogram showing distribution of annual release
    plt.figure(figsize=(12, 6))
    plt.hist(df['Year'], bins=range(int(df['Year'].min()), int(df['Year'].max())+1), color='skyblue', edgecolor='black')
    plt.xlabel('Year')
    plt.ylabel('Total Anime Released')
    plt.title('Total Anime Released Per Year')
    plt.grid(True, alpha=0.3)
    plt.show()

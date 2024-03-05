from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.multiclass import OneVsRestClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import classification_report, accuracy_score
import pandas as pd
from sklearn.preprocessing import MultiLabelBinarizer
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://root:comsc@localhost/testdb?charset=utf8mb4")



def simplify_genres(genre_list):
    simplified_genres = set()
    mapping = {
        'Action': ['Action'],
        'Adventure': ['Adventure'],
        'Fantasy': ['Fantasy'],
        'Sci-Fi': ['Sci-Fi'],
        'Space': ['Space'],        
    
    }
    for genre in genre_list:
        for simplified_genre, subgenres in mapping.items():
            if genre in subgenres:
                simplified_genres.add(simplified_genre)

    print("Original genres:", genre_list, "Simplified genres:", simplified_genres)
    return list(simplified_genres)

print(simplify_genres(['Action', 'Adult Cast', 'Award Winning', 'Sci-Fi', 'Space']))


def TFIDF_NB_model():
    query = "SELECT Lemmatised_Synopsis, Genres FROM processed_synopsis"
    df = pd.read_sql(query, engine)

    df['Simplified_Genres'] = df['Genres'].apply(lambda x: simplify_genres(x.split(', ')))
    print("Simplified genres examples:", df['Simplified_Genres'].head())
    
    if df['Simplified_Genres'].apply(lambda x: len(x) == 0).any():
        print("Some records have empty genre lists.")
   
    mlb = MultiLabelBinarizer()
    y = mlb.fit_transform(df['Simplified_Genres'])

    print("Shape of y:", y.shape)
    print("Sample labels:", y[:5])
    
    
    tfidf_vectoriser = TfidfVectorizer(min_df=0.01, max_df=0.85, stop_words=None, max_features=10000, ngram_range=(1,3))
    tfidf_matrix = tfidf_vectoriser.fit_transform(df['Lemmatised_Synopsis'])
    
    
    X_train, X_test, y_train, y_test = train_test_split(tfidf_matrix, y, test_size=0.3, random_state=42)

    model = OneVsRestClassifier(RandomForestClassifier(n_estimators=100, random_state=42))   
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    print("Classification Report:\n", classification_report(y_test, predictions))
    print("Accuracy:", accuracy_score(y_test, predictions))


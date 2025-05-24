import pandas as pd
import numpy as np
import os
import urllib.request
import random
import time
import mysql.connector
from sqlalchemy import create_engine

def download_file(url, filename):
    if not os.path.exists(filename):
        print(f"Downloading {filename}...")
        urllib.request.urlretrieve(url, filename)
        print(f"{filename} download complete.")
    else:
        print(f"{filename} already exists, skipping download.")

def load_tsv_gz(filename):
    print(f"Loading {filename}...")
    return pd.read_csv(filename, sep='\t', na_values='\\N', dtype=str, compression='gzip')

def format_duration(minutes):
    """Convert minutes to 'Xh Ym' format"""
    try:
        minutes = int(minutes)
        h = minutes // 60
        m = minutes % 60
        return f"{h}h {m}m"
    except:
        return "Unknown"

def scrape_movie_data(num_movies=250):
    """Scrape top N movies from IMDb dataset"""
    if not os.path.exists('data'):
        os.makedirs('data')

    # Download IMDb datasets
    download_file("https://datasets.imdbws.com/title.basics.tsv.gz", "title.basics.tsv.gz")
    download_file("https://datasets.imdbws.com/title.ratings.tsv.gz", "title.ratings.tsv.gz")

    basics_df = load_tsv_gz("title.basics.tsv.gz")
    ratings_df = load_tsv_gz("title.ratings.tsv.gz")

    movies_df = basics_df[basics_df['titleType'] == 'movie'].copy()
    merged_df = pd.merge(movies_df, ratings_df, on='tconst')

    merged_df['averageRating'] = merged_df['averageRating'].astype(float)
    merged_df['numVotes'] = merged_df['numVotes'].astype(int)

    filtered_df = merged_df[merged_df['numVotes'] >= 10000]
    top_movies = filtered_df.sort_values(by=['averageRating', 'numVotes'], ascending=False).head(num_movies).copy()
    top_movies.reset_index(drop=True, inplace=True)

    final_df = pd.DataFrame({
        'movie_id': top_movies.index + 1,
        'title': top_movies['primaryTitle'],
        'year': top_movies['startYear'],
        'duration': top_movies['runtimeMinutes'].apply(format_duration),
        'genre': top_movies['genres'].fillna('Unknown'),
        'imdb_rating': top_movies['averageRating'],
        'position': top_movies.index + 1
    })

    final_df.to_csv('data/movies.csv', index=False)
    print(f"Successfully saved top {num_movies} movies to data/movies.csv")
    return final_df

def generate_ratings(movies_df, num_users=50):
    """Generate user ratings with 1 decimal place"""
    if movies_df.empty:
        print("Error: No movie data available")
        return pd.DataFrame()

    print("Generating user ratings...")
    ratings = []
    movie_ids = movies_df['movie_id'].tolist()
    movie_ratings = dict(zip(movies_df['movie_id'], movies_df['imdb_rating']))

    for user_id in range(1, num_users + 1):
        try:
            num_ratings = random.randint(50, min(100, len(movie_ids)))
            rated_movies = random.sample(movie_ids, num_ratings)

            for movie_id in rated_movies:
                imdb_rating = movie_ratings[movie_id]
                # Generate rating with 1 decimal place
                rating = np.clip(
                    np.random.normal(loc=imdb_rating, scale=0.5),
                    1.0,
                    10.0
                ).round(1)  # Round to 1 decimal place

                ratings.append({
                    'user_id': int(user_id),
                    'movie_id': int(movie_id),
                    'rating': float(rating),
                    'timestamp': int(time.time()) - random.randint(0, 31536000 * 3)
                })

        except Exception as e:
            print(f"Error generating ratings for user {user_id}: {e}")
            continue

    ratings_df = pd.DataFrame(ratings)
    ratings_df.to_csv('data/ratings.csv', index=False)

    print("\nRating distribution:")
    print(ratings_df['rating'].value_counts().sort_index())
    return ratings_df

def save_to_mysql(movies_df, ratings_df):
    """Save data to MySQL database"""
    try:
        db = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root123",
            charset='utf8mb4'
        )
        cursor = db.cursor()

        # Create databases
        cursor.execute("CREATE DATABASE IF NOT EXISTS moviedata CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        cursor.execute("CREATE DATABASE IF NOT EXISTS userdata CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        print("Databases created/verified")

        # Create movies table
        cursor.execute("USE moviedata")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                movie_id INT PRIMARY KEY,
                title VARCHAR(255),
                year VARCHAR(10),
                duration VARCHAR(20),
                genre VARCHAR(255),
                imdb_rating FLOAT,
                position INT
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)

        # Insert movie data
        for _, row in movies_df.iterrows():
            cursor.execute("""
                INSERT INTO movies (movie_id, title, year, duration, genre, imdb_rating, position)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    year = VALUES(year),
                    duration = VALUES(duration),
                    genre = VALUES(genre),
                    imdb_rating = VALUES(imdb_rating),
                    position = VALUES(position)
            """, (
                int(row['movie_id']),
                str(row['title']),
                str(row['year']),
                str(row['duration']),
                str(row['genre']),
                float(row['imdb_rating']),
                int(row['position'])
            ))

        # Create ratings table with DECIMAL for 1 decimal place
        cursor.execute("USE userdata")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ratings (
                user_id INT,
                movie_id INT,
                rating DECIMAL(3,1),  # Stores values like 8.5
                timestamp INT,
                PRIMARY KEY (user_id, movie_id)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)

        # Insert rating data
        for _, row in ratings_df.iterrows():
            cursor.execute("""
                INSERT INTO ratings (user_id, movie_id, rating, timestamp)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    rating = VALUES(rating),
                    timestamp = VALUES(timestamp)
            """, (
                int(row['user_id']),
                int(row['movie_id']),
                float(row['rating']),  # Will be stored with 1 decimal place
                int(row['timestamp'])
            ))

        db.commit()
        print("Data saved successfully!")

    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'db' in locals() and db.is_connected():
            db.close()

if __name__ == '__main__':
    movies_df = scrape_movie_data(num_movies=10000)

    if not movies_df.empty:
        ratings_df = generate_ratings(movies_df, num_users=2000)

        print("\nSample movie data:")
        print(movies_df.head())

        print("\nSample rating data:")
        print(ratings_df.head())

        save_to_mysql(movies_df, ratings_df)
    else:
        print("Cannot generate ratings - no movie data available")
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
    download_file("https://datasets.imdbws.com/title.basics.tsv.gz", "data/title.basics.tsv.gz")
    download_file("https://datasets.imdbws.com/title.ratings.tsv.gz", "data/title.ratings.tsv.gz")

    basics_df = load_tsv_gz("data/title.basics.tsv.gz")
    ratings_df = load_tsv_gz("data/title.ratings.tsv.gz")

    movies_df = basics_df[basics_df['titleType'] == 'movie'].copy()
    merged_df = pd.merge(movies_df, ratings_df, on='tconst')

    merged_df['averageRating'] = merged_df['averageRating'].astype(float)
    merged_df['numVotes'] = merged_df['numVotes'].astype(int)

    filtered_df = merged_df[merged_df['numVotes'] >= 10000]
    top_movies = filtered_df.sort_values(by=['averageRating', 'numVotes'], ascending=False).head(num_movies).copy()
    top_movies.reset_index(drop=True, inplace=True)

    # 创建映射字典：tconst -> 数字movie_id
    tconst_to_movie_id = {tconst: idx + 1 for idx, tconst in enumerate(top_movies['tconst'])}

    # 创建最终输出的DataFrame（不包含tconst）
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

    # 返回final_df和映射字典
    return final_df, tconst_to_movie_id

def scrape_people_data(tconst_to_movie_id):
    """处理人物数据，生成与电影对应的CSV文件"""
    if not os.path.exists('data'):
        os.makedirs('data')

    # 下载IMDb人物数据集
    download_file("https://datasets.imdbws.com/name.basics.tsv.gz", "data/name.basics.tsv.gz")
    download_file("https://datasets.imdbws.com/title.principals.tsv.gz", "data/title.principals.tsv.gz")

    # 加载数据
    names_df = load_tsv_gz("data/name.basics.tsv.gz")
    principals_df = load_tsv_gz("data/title.principals.tsv.gz")

    # 只处理我们电影列表中的人物
    movie_tconsts = list(tconst_to_movie_id.keys())
    principals_df = principals_df[principals_df['tconst'].isin(movie_tconsts)]

    # 合并人物信息
    merged_people = pd.merge(
        principals_df,
        names_df,
        on='nconst'
    )

    # 添加movie_id列（映射到我们定义的数字ID）
    merged_people['movie_id'] = merged_people['tconst'].map(tconst_to_movie_id)

    # 筛选需要的列并去重
    people_df = merged_people[['movie_id', 'primaryName', 'birthYear', 'deathYear', 'primaryProfession']].copy()
    people_df = people_df.drop_duplicates()

    # 保存到CSV
    people_df.to_csv('data/people.csv', index=False)
    print(f"成功保存人物数据到 data/people.csv，共 {len(people_df)} 条记录")
    return people_df

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

def save_to_mysql(movies_df, ratings_df, people_df=None, comments_df=None, feedback_df=None):
    """Save all data to MySQL database projectdata, including admins and users table."""
    import mysql.connector

    try:
        db = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root123",
            charset='utf8mb4'
        )
        cursor = db.cursor()

        # 创建数据库
        cursor.execute("CREATE DATABASE IF NOT EXISTS projectdata CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        cursor.execute("USE projectdata")

        # 1. 管理员表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                admin_id INT PRIMARY KEY AUTO_INCREMENT,
                username VARCHAR(32) UNIQUE,
                password VARCHAR(64)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        cursor.execute("""
            INSERT IGNORE INTO admins (admin_id, username, password)
            VALUES (1, 'admin', 'password')
        """)

        # 2. 用户表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INT PRIMARY KEY,
                password VARCHAR(64)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        user_ids = ratings_df['user_id'].unique() if ratings_df is not None else []
        for uid in user_ids:
            cursor.execute("""
                INSERT IGNORE INTO users (user_id, password)
                VALUES (%s, %s)
            """, (int(uid), "user"))

        # 3. 电影表
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

        # 4. 评分表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ratings (
                user_id INT,
                movie_id INT,
                rating DECIMAL(3,1),
                timestamp INT,
                PRIMARY KEY (user_id, movie_id)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
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
                float(row['rating']),
                int(row['timestamp'])
            ))

        # 5. 人物表
        if people_df is not None:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS people (
                    movie_id INT,
                    primaryName VARCHAR(255),
                    birthYear VARCHAR(10),
                    deathYear VARCHAR(10),
                    primaryProfession TEXT,
                    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            """)
            for _, row in people_df.iterrows():
                cursor.execute("""
                    INSERT INTO people (movie_id, primaryName, birthYear, deathYear, primaryProfession)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        primaryName = VALUES(primaryName),
                        birthYear = VALUES(birthYear),
                        deathYear = VALUES(deathYear),
                        primaryProfession = VALUES(primaryProfession)
                """, (
                    int(row['movie_id']),
                    str(row['primaryName']),
                    str(row['birthYear']),
                    str(row['deathYear']),
                    str(row['primaryProfession'])
                ))

        # 6. 评论表
        if comments_df is not None:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS comments (
                    comment_id INT PRIMARY KEY,
                    movie_id INT NOT NULL,
                    user_id INT NOT NULL,
                    content TEXT NOT NULL,
                    comment_time DATETIME NOT NULL
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            """)
            for _, row in comments_df.iterrows():
                cursor.execute("""
                    INSERT INTO comments (comment_id, movie_id, user_id, content, comment_time)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        movie_id=VALUES(movie_id),
                        user_id=VALUES(user_id),
                        content=VALUES(content),
                        comment_time=VALUES(comment_time)
                """, (
                    int(row['comment_id']),
                    int(row['movie_id']),
                    int(row['user_id']),
                    str(row['content']),
                    str(row['comment_time'])
                ))

        # 7. 反馈表
        if feedback_df is not None:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS feedback (
                    id INT PRIMARY KEY,
                    sender_id VARCHAR(32) NOT NULL,
                    receiver_id VARCHAR(32) NOT NULL,
                    content TEXT NOT NULL,
                    create_time VARCHAR(32) NOT NULL,
                    is_read TINYINT DEFAULT 0,
                    type VARCHAR(16)
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            """)
            for _, row in feedback_df.iterrows():
                cursor.execute("""
                    INSERT INTO feedback (id, sender_id, receiver_id, content, create_time, is_read, type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        sender_id=VALUES(sender_id),
                        receiver_id=VALUES(receiver_id),
                        content=VALUES(content),
                        create_time=VALUES(create_time),
                        is_read=VALUES(is_read),
                        type=VALUES(type)
                """, (
                    int(row['id']),
                    str(row['sender_id']),
                    str(row['receiver_id']),
                    str(row['content']),
                    str(row['create_time']),
                    int(row['is_read']),
                    str(row['type']) if 'type' in row and pd.notna(row['type']) else None
                ))

        db.commit()
        print("All data saved successfully to projectdata!")

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
    # 获取电影数据和tconst到movie_id的映射
    movies_df, tconst_to_movie_id = scrape_movie_data(num_movies=11809)

    if not movies_df.empty:
        # 处理人物数据
        people_df = scrape_people_data(tconst_to_movie_id)

        # 生成评分数据
        ratings_df = generate_ratings(movies_df, num_users=4000)

        # 读取评论数据
        comments_df = pd.read_csv('data/comments.csv')

        # 读取反馈数据
        feedback_df = pd.read_csv('data/feedback.csv')

        print("\n电影数据示例:")
        print(movies_df.head())

        print("\n人物数据示例:")
        print(people_df.head())

        print("\n评分数据示例:")
        print(ratings_df.head())

        print("\n评论数据示例:")
        print(comments_df.head())

        print("\n反馈数据示例:")
        print(feedback_df.head())

        # 保存到MySQL（包括人物、评论、反馈数据）
        save_to_mysql(movies_df, ratings_df, people_df, comments_df, feedback_df)
    else:
        print("无法生成数据 - 没有可用的电影数据")

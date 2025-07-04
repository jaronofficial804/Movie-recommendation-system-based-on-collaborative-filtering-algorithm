import pandas as pd
import os
from datetime import datetime

COMMENTS_PATH = 'data/comments.csv'

def load_comments():
    if os.path.exists(COMMENTS_PATH):
        return pd.read_csv(COMMENTS_PATH)
    else:
        return pd.DataFrame(columns=['comment_id', 'movie_id', 'user_id', 'content', 'comment_time'])

def save_comments(df):
    df.to_csv(COMMENTS_PATH, index=False)


def get_movie_details(movie_ids, movies_df):
    """根据电影ID获取详细信息"""
    return movies_df[movies_df['movie_id'].isin(movie_ids)]


def get_user_rated_movies(user_id, ratings_df, movies_df):
    """获取用户已评分的电影"""
    user_ratings = ratings_df[ratings_df['user_id'] == user_id]
    return pd.merge(user_ratings, movies_df, on='movie_id').sort_values('rating', ascending=False)


def get_top_rated_movies(movies_df, ratings_df, n=10):
    """获取评分最高的电影"""
    # 计算每部电影的平均评分
    avg_ratings = ratings_df.groupby('movie_id')['rating'].mean().reset_index()
    avg_ratings = avg_ratings.rename(columns={'rating': 'avg_rating'})

    # 合并电影信息和平均评分
    merged = pd.merge(movies_df, avg_ratings, on='movie_id')

    # 按平均评分降序排序并返回前n部
    return merged.sort_values('avg_rating', ascending=False).head(n)
import pandas as pd
import os
from datetime import datetime

FEEDBACK_PATH = 'data/feedback.csv'

def load_feedback():
    if os.path.exists(FEEDBACK_PATH):
        return pd.read_csv(FEEDBACK_PATH)
    else:
        # 初始空表
        return pd.DataFrame(columns=['id', 'sender_id', 'receiver_id', 'content', 'create_time', 'is_read', 'type'])

def save_feedback(df):
    df.to_csv(FEEDBACK_PATH, index=False)

import pandas as pd
from datetime import datetime


def load_data():
    """加载电影和评分数据"""
    try:
        movies = pd.read_csv('data/movies.csv')
        ratings = pd.read_csv('data/ratings.csv')

        # 处理时长数据 - 针对"Xh Ym"格式
        def parse_duration(duration):
            if pd.isna(duration):
                return 0
            parts = duration.split()
            hours = int(parts[0].replace('h', '')) if 'h' in parts[0] else 0
            minutes = int(parts[1].replace('m', '')) if len(parts) > 1 and 'm' in parts[1] else 0
            return hours * 60 + minutes

        movies['duration_min'] = movies['duration'].apply(parse_duration)

        # 转换时间戳
        ratings['rating_date'] = pd.to_datetime(ratings['timestamp'], unit='s')

        return movies, ratings
    except FileNotFoundError as e:
        print(f"数据加载错误: {e}")
        return None, None
    except Exception as e:
        print(f"数据处理错误: {e}")
        return None, None


def preprocess_data(movies, ratings):
    """预处理数据"""
    try:
        # 合并数据
        movie_ratings = pd.merge(ratings, movies, on='movie_id')

        # 用户-电影评分矩阵
        user_movie_ratings = ratings.pivot_table(
            index='user_id',
            columns='movie_id',
            values='rating'
        ).fillna(0)

        # 电影信息
        movie_info = movies.set_index('movie_id')

        return movie_ratings, user_movie_ratings, movie_info
    except Exception as e:
        print(f"数据预处理错误: {e}")
        return None, None, None
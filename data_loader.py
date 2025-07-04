"""
数据加载与预处理模块
包含加载电影和评分数据的函数以及数据预处理功能
"""

import pandas as pd
from datetime import datetime


def load_data():
    """
    加载并处理电影和评分数据

    返回:
        movies (DataFrame): 处理后的电影数据
        ratings (DataFrame): 处理后的评分数据

    处理步骤:
        1. 从CSV文件加载原始数据
        2. 处理电影时长格式（将"Xh Ym"转换为分钟数）
        3. 转换评分时间戳为可读日期格式
    """
    try:
        # 加载原始数据
        movies = pd.read_csv('data/movies.csv')
        ratings = pd.read_csv('data/ratings.csv')

        # 自定义函数：解析电影时长（"Xh Ym"格式）
        def parse_duration(duration):
            """将电影时长字符串解析为分钟数"""
            if pd.isna(duration):
                return 0  # 缺失值处理

            parts = duration.split()
            # 提取小时部分
            hours = int(parts[0].replace('h', '')) if 'h' in parts[0] else 0
            # 提取分钟部分（如果有）
            minutes = int(parts[1].replace('m', '')) if len(parts) > 1 and 'm' in parts[1] else 0
            return hours * 60 + minutes

        # 应用时长解析函数
        movies['duration_min'] = movies['duration'].apply(parse_duration)

        # 转换时间戳为日期格式
        ratings['rating_date'] = pd.to_datetime(ratings['timestamp'], unit='s')

        return movies, ratings

    except FileNotFoundError as e:
        print(f"数据加载错误: 文件未找到 - {e}")
        return None, None
    except pd.errors.EmptyDataError as e:
        print(f"数据加载错误: 文件内容为空 - {e}")
        return None, None
    except Exception as e:
        print(f"数据处理错误: {e}")
        return None, None


def preprocess_data(movies, ratings):
    """
    数据预处理：创建用于推荐系统的结构化数据

    参数:
        movies (DataFrame): 电影数据
        ratings (DataFrame): 评分数据

    返回:
        movie_ratings (DataFrame): 合并后的电影评分数据
        user_movie_ratings (DataFrame): 用户-电影评分矩阵
        movie_info (DataFrame): 以电影ID为索引的电影信息

    处理步骤:
        1. 合并电影和评分数据
        2. 创建用户-电影评分矩阵（稀疏矩阵填充为0）
        3. 创建以电影ID为索引的电影信息表
    """
    try:
        # 合并电影和评分数据
        movie_ratings = pd.merge(ratings, movies, on='movie_id')

        # 创建用户-电影评分矩阵（pivot table）
        # 行: user_id, 列: movie_id, 值: rating
        user_movie_ratings = ratings.pivot_table(
            index='user_id',
            columns='movie_id',
            values='rating',
            fill_value=0  # 用0填充缺失值（表示该用户未评分的电影）
        )

        # 创建以电影ID为索引的电影信息表
        movie_info = movies.set_index('movie_id')

        return movie_ratings, user_movie_ratings, movie_info

    except KeyError as e:
        print(f"数据预处理错误: 缺少必要列 - {e}")
        return None, None, None
    except ValueError as e:
        print(f"数据预处理错误: 值错误 - {e}")
        return None, None, None
    except Exception as e:
        print(f"数据预处理错误: {e}")
        return None, None, None
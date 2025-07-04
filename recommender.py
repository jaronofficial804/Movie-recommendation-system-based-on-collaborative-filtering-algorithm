"""
基于用户的协同过滤推荐系统实现（带Redis缓存）
使用皮尔逊相关系数计算用户相似度，生成个性化电影推荐
"""

import numpy as np
import pandas as pd
import redis
import json
import zlib
from sklearn.metrics.pairwise import cosine_similarity
from functools import lru_cache
import os


class UserCFRecommender:
    def __init__(self, user_movie_ratings, movie_info, redis_host='localhost', redis_port=6379, redis_db=0):
        """
        初始化推荐系统（带Redis缓存支持）

        参数:
            user_movie_ratings (DataFrame): 用户-电影评分矩阵（行:用户, 列:电影）
            movie_info (DataFrame): 电影元数据信息
            redis_host (str): Redis服务器地址
            redis_port (int): Redis端口
            redis_db (int): Redis数据库编号

        属性:
            user_movie_ratings: 处理后的评分矩阵（未评分项设为NaN）
            movie_info: 电影信息数据集
            user_similarity: 用户相似度矩阵
            redis_client: Redis连接客户端
            cache_key: 相似度矩阵缓存键名
            chunk_size: 分块存储时的块大小
        """
        # 数据初始化
        self.user_movie_ratings = user_movie_ratings.replace(0, np.nan)
        self.movie_info = movie_info
        self.user_similarity = None

        # Redis配置
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', redis_host),
            port=int(os.getenv('REDIS_PORT', redis_port)),
            password=os.getenv('REDIS_PASSWORD', None),
            db=redis_db,
            decode_responses=False,  # 原始字节模式，便于压缩
            socket_timeout=10,
            retry_on_timeout=True
        )
        self.cache_key = "user_sim_matrix"
        self.chunk_size = 1000  # 分块大小（用户数超过此值时自动分块存储）

        # 连接测试
        try:
            self.redis_client.ping()
        except redis.ConnectionError:
            print("警告: 无法连接Redis服务器，将禁用缓存功能")
            self.redis_client = None

    def _serialize_matrix(self, matrix):
        """序列化相似度矩阵（使用压缩优化）"""
        return zlib.compress(json.dumps(matrix.tolist()).encode('utf-8'))

    def _deserialize_matrix(self, data):
        """反序列化相似度矩阵"""
        return np.array(json.loads(zlib.decompress(data).decode('utf-8')))

    def _store_matrix_chunks(self, matrix):
        """
        分块存储大型相似度矩阵
        适用于用户量>1万的场景
        """
        n_users = len(matrix)
        for i in range(0, n_users, self.chunk_size):
            chunk = matrix[i:i + self.chunk_size, :]
            chunk_key = f"{self.cache_key}:chunk_{i // self.chunk_size}"
            self.redis_client.setex(
                chunk_key,
                86400,  # 24小时TTL
                self._serialize_matrix(chunk)
            )
        # 存储元数据
        meta = {
            'n_chunks': (n_users // self.chunk_size) + 1,
            'shape': matrix.shape,
            'chunk_size': self.chunk_size
        }
        self.redis_client.setex(
            f"{self.cache_key}:meta",
            86400,
            json.dumps(meta)
        )

    def _load_matrix_chunks(self):
        """加载分块存储的相似度矩阵"""
        meta = json.loads(self.redis_client.get(f"{self.cache_key}:meta"))
        matrix = np.zeros(meta['shape'])

        for i in range(meta['n_chunks']):
            chunk_key = f"{self.cache_key}:chunk_{i}"
            chunk_data = self.redis_client.get(chunk_key)
            if chunk_data:
                start = i * meta['chunk_size']
                end = min((i + 1) * meta['chunk_size'], meta['shape'][0])
                matrix[start:end, :] = self._deserialize_matrix(chunk_data)

        return matrix

    def calculate_similarity(self, force_recompute=False):
        """
        计算用户相似度矩阵（带Redis缓存支持）

        参数:
            force_recompute (bool): 是否强制重新计算（忽略缓存）
        """
        # 如果强制重新计算，先清除缓存
        if force_recompute:
            self.clear_cache()

        # 尝试从Redis加载缓存
        if self.redis_client and not force_recompute:
            try:
                # 检查分块存储模式
                if self.redis_client.exists(f"{self.cache_key}:meta"):
                    print("[Redis] 从分块缓存加载相似度矩阵...")
                    self.user_similarity = self._load_matrix_chunks()
                    return

                # 检查整体存储模式
                cached_data = self.redis_client.get(self.cache_key)
                if cached_data:
                    print("[Redis] 从缓存加载相似度矩阵")
                    self.user_similarity = self._deserialize_matrix(cached_data)
                    return
            except Exception as e:
                print(f"[Redis] 缓存加载失败: {e}, 将重新计算")

        # 无缓存或加载失败时重新计算
        print("计算用户相似度矩阵...")
        norm_ratings = self.user_movie_ratings.sub(self.user_movie_ratings.mean(axis=1), axis=0)
        filled = norm_ratings.fillna(0)

        # 计算皮尔逊相关系数
        numerator = filled @ filled.T
        denominator = np.sqrt((filled ** 2).sum(axis=1).values[:, None] @
                              (filled ** 2).sum(axis=1).values[None, :])

        denominator[denominator == 0] = np.inf
        self.user_similarity = numerator / denominator

        # 确保是NumPy数组（修复关键错误）
        if isinstance(self.user_similarity, pd.DataFrame):
            self.user_similarity = self.user_similarity.values

        # 将对角线设为零（排除自相似度）
        np.fill_diagonal(self.user_similarity, 0)

        # 存储到Redis
        if self.redis_client:
            try:
                n_users = len(self.user_similarity)

                # 大型矩阵使用分块存储
                if n_users > self.chunk_size:
                    print(f"[Redis] 分块存储相似度矩阵 ({n_users}用户)...")
                    self._store_matrix_chunks(self.user_similarity)
                else:
                    # 小型矩阵整体存储
                    print("[Redis] 存储相似度矩阵到缓存...")
                    self.redis_client.setex(
                        self.cache_key,
                        86400,  # 24小时TTL
                        self._serialize_matrix(self.user_similarity)
                    )
            except Exception as e:
                print(f"[Redis] 缓存存储失败: {e}")
    def clear_cache(self):
        """清除Redis中的相似度矩阵缓存"""
        if self.redis_client:
            # 删除所有相关键
            keys = self.redis_client.keys(f"{self.cache_key}*")
            if keys:
                self.redis_client.delete(*keys)
                print("[Redis] 已清除相似度矩阵缓存")

    def recommend_items(self, user_id, k=5, n=10, min_similarity=0.1):
        """
        生成个性化电影推荐（带缓存优化）

        参数:
            user_id: 目标用户ID
            k: 选取的相似用户数量（默认5）
            n: 返回的推荐电影数量（默认10）
            min_similarity: 最小相似度阈值（默认0.1）

        返回:
            list: 推荐电影列表，按预测评分排序
        """
        # 确保相似度矩阵已计算
        if self.user_similarity is None:
            self.calculate_similarity()

        try:
            user_idx = self.user_movie_ratings.index.get_loc(user_id)
            user_rated = ~self.user_movie_ratings.iloc[user_idx].isna()

            # 获取有效相似用户（相似度>阈值且非自身）
            sim_scores = self.user_similarity[user_idx]
            valid_users = np.where(
                (sim_scores >= min_similarity) &
                (np.arange(len(sim_scores)) != user_idx)
            )[0]

            # 如果没有足够相似用户，使用全局热门电影
            if len(valid_users) < k:
                print(f"警告: 相似用户不足({len(valid_users)}个)，使用热门电影补充")
                return self._get_fallback_recommendations(n, user_rated)

            # 选取TopK相似用户
            top_k_users = valid_users[np.argsort(sim_scores[valid_users])[-k:]]

            # 计算加权预测评分
            weighted_sum = np.zeros(len(self.user_movie_ratings.columns))
            weight_sum = np.zeros(len(self.user_movie_ratings.columns))

            for sim_user_idx in top_k_users:
                sim_score = sim_scores[sim_user_idx]
                other_ratings = self.user_movie_ratings.iloc[sim_user_idx].values
                mask = ~np.isnan(other_ratings)

                weighted_sum[mask] += sim_score * other_ratings[mask]
                weight_sum[mask] += sim_score

            # 计算最终预测评分
            pred_ratings = np.divide(
                weighted_sum,
                weight_sum,
                out=np.zeros_like(weighted_sum),
                where=(weight_sum != 0)
            )

            # 排除已评分电影并获取TopN推荐
            unrated_mask = ~user_rated
            candidate_movies = np.where(unrated_mask)[0]
            top_n_idx = np.argsort(pred_ratings[candidate_movies])[-n:][::-1]

            return self._format_recommendations(candidate_movies, top_n_idx, pred_ratings)

        except KeyError:
            print(f"错误: 用户ID {user_id} 不存在，返回热门电影")
            return self._get_fallback_recommendations(n)
        except Exception as e:
            print(f"推荐过程中出错: {e}")
            return self._get_fallback_recommendations(n)

    def _format_recommendations(self, candidate_movies, top_n_idx, pred_ratings):
        """格式化推荐结果"""
        recommendations = []
        for idx in top_n_idx:
            movie_id = self.user_movie_ratings.columns[candidate_movies[idx]]
            movie_data = self.movie_info.loc[movie_id].to_dict()
            recommendations.append({
                'movie_id': movie_id,
                'predicted_rating': round(pred_ratings[candidate_movies[idx]], 2),
                **movie_data
            })
        return recommendations

    def _get_fallback_recommendations(self, n, exclude_rated=None):
        """
        获取后备推荐（当相似用户不足时使用）

        参数:
            n: 推荐数量
            exclude_rated: 需要排除的已评分电影掩码

        返回:
            list: 按IMDb评分降序的热门电影
        """
        if exclude_rated is not None:
            unrated_movies = self.user_movie_ratings.columns[~exclude_rated]
            movies = self.movie_info.loc[unrated_movies]
        else:
            movies = self.movie_info

        top_movies = movies.sort_values('imdb_rating', ascending=False).head(n)
        return [{
            'movie_id': idx,
            'predicted_rating': round(row['imdb_rating'], 2),
            **row.to_dict()
        } for idx, row in top_movies.iterrows()]

    def __del__(self):
        """析构时关闭Redis连接"""
        if hasattr(self, 'redis_client') and self.redis_client:
            self.redis_client.close()
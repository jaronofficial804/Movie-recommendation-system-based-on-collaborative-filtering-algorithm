import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity


class UserCFRecommender:
    def __init__(self, user_movie_ratings, movie_info):
        # 将未评分项设为NaN（保留真实0评分）
        self.user_movie_ratings = user_movie_ratings.replace(0, np.nan)
        self.movie_info = movie_info
        self.user_similarity = None

    def calculate_similarity(self):
        """改进的相似度计算，处理NaN值"""
        # 使用皮尔逊相关系数替代余弦相似度
        norm_ratings = self.user_movie_ratings.sub(self.user_movie_ratings.mean(axis=1), axis=0)
        filled = norm_ratings.fillna(0)

        # 计算皮尔逊相关系数
        numerator = filled @ filled.T
        denominator = np.sqrt((filled ** 2).sum(axis=1).values[:, None] @
                              (filled ** 2).sum(axis=1).values[None, :])

        # 避免除以零
        denominator[denominator == 0] = np.inf
        self.user_similarity = numerator / denominator

        # 确保是numpy数组（修复flat属性错误）
        if isinstance(self.user_similarity, pd.DataFrame):
            self.user_similarity = self.user_similarity.values

        # 将对角线设为零（排除自身）
        np.fill_diagonal(self.user_similarity, 0)

    def recommend_items(self, user_id, k=5, n=20):
        """改进的推荐方法"""
        if self.user_similarity is None:
            self.calculate_similarity()

        try:
            user_idx = self.user_movie_ratings.index.get_loc(user_id)
            user_rated = ~self.user_movie_ratings.iloc[user_idx].isna()

            # 获取有效相似用户（排除零相似和自身）
            sim_scores = self.user_similarity[user_idx]
            valid_users = np.where((sim_scores > 0) &
                                   (np.arange(len(sim_scores)) != user_idx))[0]
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

            # 处理除零情况
            pred_ratings = np.divide(weighted_sum, weight_sum,
                                     out=np.zeros_like(weighted_sum),
                                     where=(weight_sum != 0))

            # 排除已评分电影
            unrated_mask = ~user_rated
            candidate_movies = np.where(unrated_mask)[0]

            # 选择TopN推荐
            top_n_idx = np.argsort(pred_ratings[candidate_movies])[-n:][::-1]
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

        except Exception as e:
            print(f"推荐过程中出错: {e}")
            return []
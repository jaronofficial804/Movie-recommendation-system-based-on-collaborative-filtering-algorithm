from flask import Flask, render_template, request
from data_loader import load_data, preprocess_data
from recommender import UserCFRecommender
from utils import get_movie_details, get_user_rated_movies, get_top_rated_movies
import pandas as pd
from datetime import datetime

app = Flask(__name__)

# 加载数据
movies, ratings = load_data()
if movies is not None and ratings is not None:
    movie_ratings, user_movie_ratings, movie_info = preprocess_data(movies, ratings)
    recommender = UserCFRecommender(user_movie_ratings, movie_info)
    top_movies = get_top_rated_movies(movies, ratings)
else:
    print("无法加载数据，请确保已运行scraper.py爬取数据")


# 基础模板
@app.route('/')
def index():
    if movies is None or ratings is None:
        return render_template('error.html', message="数据加载失败，请检查数据文件")

    user_ids = ratings['user_id'].unique()
    return render_template('index.html',
                           user_ids=user_ids,
                           top_movies=top_movies.to_dict('records'))


@app.route('/recommend', methods=['POST'])
def recommend():
    if movies is None or ratings is None:
        return render_template('error.html', message="数据加载失败，请检查数据文件")

    user_id = int(request.form['user_id'])
    rated_movies = get_user_rated_movies(user_id, ratings, movies)
    recommendations = recommender.recommend_items(user_id)

    return render_template('recommendations.html',
                           user_id=user_id,
                           rated_movies=rated_movies.to_dict('records'),
                           recommendations=recommendations)


@app.route('/admin')
def admin_dashboard():
    if movies is None or ratings is None:
        return render_template('error.html', message="数据加载失败，请检查数据文件")

    # 基本统计信息
    stats = {
        'total_movies': len(movies),
        'total_users': len(ratings['user_id'].unique()),
        'total_ratings': len(ratings),
        'avg_rating': ratings['rating'].mean(),
        'first_rating': ratings['rating_date'].min().strftime('%Y-%m-%d'),
        'last_rating': ratings['rating_date'].max().strftime('%Y-%m-%d')
    }

    # 时长分布
    duration_bins = ['<90分钟', '90-120分钟', '120-150分钟', '>150分钟']
    duration_cuts = pd.cut(
        movies['duration_min'],
        bins=[0, 90, 120, 150, 1000],
        labels=duration_bins
    )
    duration_counts = duration_cuts.value_counts().reindex(duration_bins, fill_value=0)

    # 评分分布
    rating_distribution = [
        len(ratings[ratings['rating'] == 5]),
        len(ratings[ratings['rating'] == 6]),
        len(ratings[ratings['rating'] == 7]),
        len(ratings[ratings['rating'] == 8]),
        len(ratings[ratings['rating'] == 9])
    ]

    # 评分时间趋势
    ratings_by_month = ratings.set_index('rating_date').resample('M')['rating'].count()

    # 活跃用户
    active_users = ratings['user_id'].value_counts().head(10)

    # 类型统计 - 分割复合类型
    all_genres = []
    for genres in movies['genre']:
        if pd.notna(genres):
            all_genres.extend([g.strip() for g in genres.split(',')])

    genre_counts = pd.Series(all_genres).value_counts()

    return render_template('admin.html',
                           stats=stats,
                           duration_labels=duration_bins,
                           duration_data=duration_counts.tolist(),
                           rating_distribution=rating_distribution,
                           time_labels=ratings_by_month.index.strftime('%Y-%m').tolist(),
                           time_data=ratings_by_month.values.tolist(),
                           active_users_labels=[f"用户 {uid}" for uid in active_users.index],
                           active_users_data=active_users.values.tolist(),
                           genre_labels=genre_counts.index.tolist(),
                           genre_data=genre_counts.values.tolist())


if __name__ == '__main__':
    app.run(debug=True)
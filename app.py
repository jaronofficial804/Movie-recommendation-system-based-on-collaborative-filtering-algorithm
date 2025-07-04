"""
Flask电影推荐系统应用
包含用户推荐、后台管理、数据统计等功能
"""

from flask import Flask, render_template, request, redirect, url_for
from data_loader import load_data, preprocess_data
from recommender import UserCFRecommender
from utils import get_movie_details, get_user_rated_movies, get_top_rated_movies, save_comments, load_comments, \
    save_feedback, load_feedback
import pandas as pd
from datetime import datetime
import os  # 添加这行
from flask import session
from flask import jsonify
# 初始化Flask应用
app = Flask(__name__)
app.secret_key = 'your_secret_key_here'

# 全局变量初始化
movies, ratings = None, None  # 存储电影和评分数据
movie_ratings, user_movie_ratings, movie_info = None, None, None  # 预处理后的数据
recommender, top_movies = None, None  # 推荐系统和热门电影


def initialize_data():
    """
    初始化加载并预处理所有数据
    包括加载CSV文件、预处理数据集和初始化推荐系统
    """
    global movies, ratings, movie_ratings, user_movie_ratings, movie_info, recommender, top_movies

    # 加载原始数据
    movies, ratings = load_data()

    # 确保数据加载成功
    if movies is not None and ratings is not None:
        # 预处理数据
        movie_ratings, user_movie_ratings, movie_info = preprocess_data(movies, ratings)

        # 初始化基于用户的协同过滤推荐系统
        recommender = UserCFRecommender(user_movie_ratings, movie_info)

        # 获取热门电影（基于平均评分）
        top_movies = get_top_rated_movies(movies, ratings)
    else:
        # 数据加载失败处理
        print("无法加载数据，请确保已运行scraper.py爬取数据")


# 首次启动时初始化数据
initialize_data()


# ======================== 前台用户路由 ========================
from flask import session

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        role = request.form.get('role')
        username = request.form.get('username')
        password = request.form.get('password')

        if role == 'admin' and username == 'admin' and password == 'password':
            session['logged_in'] = True
            session['role'] = 'admin'
            return redirect(url_for('admin_dashboard'))

        elif role == 'user' and password == 'user':
            try:
                user_id = int(username)
                if user_id in ratings['user_id'].unique():
                    session['logged_in'] = True
                    session['role'] = 'user'
                    session['user_id'] = user_id
                    return redirect(url_for('recommend_user'))
                else:
                    return render_template('login.html', error="用户ID不存在")
            except ValueError:
                return render_template('login.html', error="用户ID格式错误")

        return render_template('login.html', error="账号或密码错误")

    return render_template('login.html')


@app.route('/')
def index():
    if not session.get('logged_in') or session.get('role') != 'user':
        return redirect(url_for('login'))

    top_movies_list = top_movies.to_dict('records')

    # 读取猫眼热搜榜
    news_path = '猫眼热点榜top10.csv'
    try:
        hot_news_list = pd.read_csv(news_path, encoding='utf-8').to_dict(orient="records")
        for item in hot_news_list:
            item['link'] = item['link'].replace("https://www.maoyan.comhttps://www.maoyan.com", "https://www.maoyan.com")
    except Exception as e:
        print(f"读取猫眼榜失败：{e}")
        hot_news_list = []

    boxoffice_path = '猫眼票房榜top10.csv'
    try:
        boxoffice_list = pd.read_csv(boxoffice_path, encoding='utf-8').to_dict(orient="records")
    except Exception as e:
        print(f"读取票房榜失败：{e}")
        boxoffice_list = []
    # ★★★ 这里添加 celebrities 示例数据 ★★★
    celebrities = [
        {"img": "https://ts2.tc.mm.bing.net/th/id/OIP-C.Se7CSHhq-56d3HFLAqthzwHaJ4?r=0&rs=1&pid=ImgDetMain&o=7&rm=3",
         "name": "Danielle Campbell", "rank": 11, "change": 29},
        {"img": "https://ts1.tc.mm.bing.net/th/id/OIP-C.JA3JYKo7RHZK8V312ffOrQHaLH?r=0&rs=1&pid=ImgDetMain&o=7&rm=3",
         "name": "Jodie Comer", "rank": 12, "change": 8},
        {"img": "https://ts2.tc.mm.bing.net/th/id/OIP-C.PTv9iq3WgO34f9aXZxEMnAHaHa?r=0&rs=1&pid=ImgDetMain&o=7&rm=3",
         "name": "Nico Parker", "rank": 13, "change": -11},
        {"img": "https://tse4-mm.cn.bing.net/th?id=OIF-C.huTSSq5%2fIeiUuu73j71wVw&r=0&rs=1&pid=ImgDetMain&o=7&rm=3",
         "name": "Danny Boyle", "rank": 14, "change": -4},
        {
            "img": "https://tse4-mm.cn.bing.net/th/id/OIF-C.3TEJlAJbg6Aygs6TqdGmkQ?r=0&o=7rm=3&rs=1&pid=ImgDetMain&o=7&rm=3",
            "name": "Jensen Ackles", "rank": 15, "change": 200},
        {
            "img": "https://tse1-mm.cn.bing.net/th/id/OIF-C.FK1d8CEiKG95Xh7ORCCzDA?r=0&o=7rm=3&rs=1&pid=ImgDetMain&o=7&rm=3",
            "name": "Denis Villeneuve", "rank": 16, "change": 408},

        {"img": "https://tse2-mm.cn.bing.net/th/id/OIP-C.bdSJWQlvKC2tc-yOm3qOHQHaJ4?r=0&o=7rm=3&rs=1&pid=ImgDetMain&o=7&rm=3",
         "name": "Zendaya", "rank": 17, "change": 12},
        {"img": "https://tse4-mm.cn.bing.net/th/id/OIP-C.i_nXZt8PmQ0c2AaYBexUOQHaIy?r=0&o=7rm=3&rs=1&pid=ImgDetMain&o=7&rm=3",
         "name": "Tom Holland", "rank": 18, "change": 4},
        {"img": "https://images.mubicdn.net/images/cast_member/51060/cache-2852-1482873231/image-w856.jpg?size=800x",
         "name": "Emma Stone", "rank": 19, "change": 3},
        {"img": "https://tse4-mm.cn.bing.net/th/id/OIP-C.vzBWHaBnndPPl7etn9bf4AHaJ4?r=0&o=7rm=3&rs=1&pid=ImgDetMain&o=7&rm=3",
         "name": "Ryan Gosling", "rank": 20, "change": 7},
    ]

    return render_template(
        'index.html',
        top_movies=top_movies_list,
        active_tab='hot',
        hot_news_list=hot_news_list,
        celebrities=celebrities,  # ★★★
        boxoffice_list=boxoffice_list  # ★★★ 这里也加上
    )



@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))



@app.route('/recommend', methods=['POST'])
def recommend():
    """
    生成推荐结果路由
    根据用户ID生成个性化推荐
    """
    if movies is None or ratings is None:
        return render_template('error.html', message="数据加载失败，请检查数据文件")

    # 从表单获取用户ID
    user_id = int(request.form['user_id'])

    # 获取用户已评分的电影
    rated_movies = get_user_rated_movies(user_id, ratings, movies)

    # 生成推荐电影列表
    recommendations = recommender.recommend_items(user_id)

    return render_template('recommendations.html',
                           user_id=user_id,
                           rated_movies=rated_movies.to_dict('records'),
                           recommendations=recommendations)
@app.route('/recommend_user')
def recommend_user():
    if not session.get('logged_in') or session.get('role') != 'user':
        return redirect(url_for('login'))

    user_id = session.get('user_id')
    rated_movies = get_user_rated_movies(user_id, ratings, movies)
    recommendations = recommender.recommend_items(user_id)
    return render_template(
        'recommendations.html',
        user_id=user_id,
        rated_movies=rated_movies.to_dict('records'),
        recommendations=recommendations,
        active_tab='recommend'
    )



# ======================== 后台管理路由 ========================

@app.route('/admin/dashboard')
def admin_dashboard():
    if not session.get('logged_in') or session.get('role') != 'admin':
        return redirect(url_for('login'))

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

    # 电影时长分布统计
    duration_bins = ['<90分钟', '90-120分钟', '120-150分钟', '>150分钟']
    duration_cuts = pd.cut(
        movies['duration_min'],
        bins=[0, 90, 120, 150, 1000],
        labels=duration_bins
    )
    duration_counts = duration_cuts.value_counts().reindex(duration_bins, fill_value=0)

    # 评分分布统计
    rating_bins = [1, 3, 5, 7, 9, 10]
    rating_labels = ['1-3分', '3-5分', '5-7分', '7-9分', '9-10分']
    rating_distribution = pd.cut(
        ratings['rating'],
        bins=rating_bins,
        labels=rating_labels,
        right=False
    ).value_counts().sort_index().tolist()

    # 评分时间趋势（按月统计）
    ratings_by_month = ratings.set_index('rating_date').resample('M')['rating'].count()

    # 活跃用户统计（评分最多的前10名用户）
    active_users = ratings['user_id'].value_counts().head(10)

    # 电影类型统计
    all_genres = []
    for genres in movies['genre']:
        if pd.notna(genres):
            all_genres.extend([g.strip() for g in genres.split(',')])

    genre_counts = pd.Series(all_genres).value_counts()

    # 渲染仪表盘模板并传递所有统计数据和图表
    return render_template('admin_dashboard.html',
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


# ======================== 电影管理路由 ========================

@app.route('/admin/movies')
def admin_movies():
    """
    电影管理列表路由
    分页展示所有电影
    """
    if movies is None or ratings is None:
        return render_template('error.html', message="数据加载失败，请检查数据文件")

    # 分页处理
    page = request.args.get('page', 1, type=int)
    per_page = 20
    total_pages = (len(movies) + per_page - 1) // per_page
    paginated_movies = movies.iloc[(page - 1) * per_page: page * per_page].to_dict('records')

    return render_template('admin_movies.html',
                           movies=paginated_movies,
                           current_page=page,
                           total_pages=total_pages)


@app.route('/admin/people_stats')
def admin_people_stats():
    """电影人物寿命统计分析路由（无寿命区间限制，统计所有有数据的人）"""
    if movies is None or ratings is None:
        return render_template('error.html', message="数据加载失败，请检查数据文件")

    try:
        # 检查文件是否存在
        people_csv_path = os.path.join('data', 'people.csv')
        if not os.path.exists(people_csv_path):
            return render_template('error.html', message=f"找不到人物数据文件: {people_csv_path}")

        # 加载数据
        people_df = pd.read_csv(people_csv_path)

        # 1. 职业分布统计
        all_professions = []
        for professions in people_df['primaryProfession']:
            if pd.notna(professions):
                all_professions.extend([p.strip() for p in str(professions).split(',')])
        profession_counts = pd.Series(all_professions).value_counts().head(15)

        # 2. 出生年代分布
        people_df['birthYear'] = pd.to_numeric(people_df['birthYear'], errors='coerce')
        valid_birth_years = people_df[people_df['birthYear'].notna()]['birthYear']
        birth_decades = (valid_birth_years // 10 * 10).astype(int)
        decade_counts = birth_decades.value_counts().sort_index()

        # 3. 全量寿命统计（无任何区间限制，只要有出生和死亡年即可）
        people_df['deathYear'] = pd.to_numeric(people_df['deathYear'], errors='coerce')
        def calculate_lifespan(row):
            try:
                birth = int(row['birthYear']) if pd.notna(row['birthYear']) else None
                death = int(row['deathYear']) if pd.notna(row['deathYear']) else None
                if birth is not None and death is not None and birth <= death:
                    return death - birth
                return None
            except:
                return None

        people_df['lifespan'] = people_df.apply(calculate_lifespan, axis=1)
        valid_lifespans = people_df[people_df['lifespan'].notna()]['lifespan']

        # 计算统计指标
        if not valid_lifespans.empty:
            lifespan_stats = [
                round(valid_lifespans.mean(), 1),
                round(valid_lifespans.median(), 1),
                int(valid_lifespans.max())
            ]
        else:
            lifespan_stats = ["无数据", "无数据", "无数据"]

        # 4. 电影人物数量最多的电影
        movie_person_counts = people_df['movie_id'].value_counts().head(10)
        top_movies_with_people = []
        for movie_id, count in movie_person_counts.items():
            movie_title = movies[movies['movie_id'] == movie_id]['title'].values[0] if not movies[
                movies['movie_id'] == movie_id].empty else f"电影ID {movie_id}"
            top_movies_with_people.append({
                'movie_id': int(movie_id),
                'title': str(movie_title),
                'count': int(count)
            })

        return render_template('admin_people_stats.html',
                               profession_labels=profession_counts.index.astype(str).tolist(),
                               profession_data=profession_counts.values.tolist(),
                               decade_labels=decade_counts.index.astype(str).tolist(),
                               decade_data=decade_counts.values.tolist(),
                               lifespan_stats=lifespan_stats,
                               top_movies=top_movies_with_people)

    except Exception as e:
        return render_template('error.html', message=f"加载人物数据时出错: {str(e)}")



@app.route('/admin/movie/<int:movie_id>')
def admin_movie_detail(movie_id):
    """电影详情管理路由（包含人物信息）"""
    if movies is None or ratings is None:
        return render_template('error.html', message="数据加载失败，请检查数据文件")

    try:
        # 获取指定电影
        movie = movies[movies['movie_id'] == movie_id].iloc[0].to_dict()

        # 获取该电影的评分数据
        movie_ratings = ratings[ratings['movie_id'] == movie_id]

        # 计算评分分布
        rating_dist = {}
        if not movie_ratings.empty:
            rating_dist = pd.cut(
                movie_ratings['rating'],
                bins=[1, 3, 5, 7, 9, 10],
                labels=['1-3分', '3-5分', '5-7分', '7-9分', '9-10分'],
                right=False
            ).value_counts().sort_index().to_dict()

        # 获取相关人物信息
        people_info = []
        people_path = os.path.join('data', 'people.csv')
        if os.path.exists(people_path):
            people_df = pd.read_csv(people_path)
            movie_people = people_df[people_df['movie_id'] == movie_id]

            if not movie_people.empty:
                people_info = movie_people[[
                    'primaryName',
                    'birthYear',
                    'deathYear',
                    'primaryProfession'
                ]].fillna('未知').to_dict('records')

        return render_template('admin_movie_detail.html',
                               movie=movie,
                               rating_dist=rating_dist,
                               people_info=people_info)

    except IndexError:
        return render_template('error.html', message="找不到指定的电影")
    except Exception as e:
        return render_template('error.html', message=f"获取电影详情时出错: {str(e)}")


@app.route('/admin/add_movie', methods=['GET', 'POST'])
def admin_add_movie():
    """
    添加新电影路由
    GET：显示添加表单
    POST：处理表单提交并保存新电影
    """
    if movies is None or ratings is None:
        return render_template('error.html', message="数据加载失败，请检查数据文件")

    if request.method == 'POST':
        try:
            # 从表单获取新电影数据
            new_movie = {
                'movie_id': int(request.form['movie_id']),
                'title': request.form['title'],
                'year': int(request.form['year']),
                'duration': request.form['duration'],
                'genre': request.form['genre'],
                'imdb_rating': float(request.form['imdb_rating']),
                'position': int(request.form['position'])
            }

            # 添加到DataFrame
            new_row = pd.DataFrame([new_movie])
            updated_movies = pd.concat([movies, new_row], ignore_index=True)

            # 保存到CSV
            updated_movies.to_csv('data/movies.csv', index=False)

            # 重新加载数据
            initialize_data()

            # 重定向到新电影的详情页
            return redirect(url_for('admin_movie_detail', movie_id=new_movie['movie_id']))
        except Exception as e:
            return render_template('error.html', message=f"添加电影失败: {str(e)}")

    # 显示添加电影表单
    return render_template('admin_add_movie.html')


@app.route('/admin/edit_movie/<int:movie_id>', methods=['GET', 'POST'])
def admin_edit_movie(movie_id):
    """
    编辑电影信息路由
    GET：显示编辑表单
    POST：处理表单提交并更新电影信息
    """
    if movies is None:
        return render_template('error.html', message="数据加载失败，请检查数据文件")

    # 获取要编辑的电影
    movie = movies[movies['movie_id'] == movie_id].iloc[0].to_dict()

    if request.method == 'POST':
        try:
            # 从表单获取更新后的电影数据
            updated_movie = {
                'movie_id': movie_id,
                'title': request.form['title'],
                'year': int(request.form['year']),
                'duration': request.form['duration'],
                'genre': request.form['genre'],
                'imdb_rating': float(request.form['imdb_rating']),
                'position': int(request.form['position'])
            }

            # 更新DataFrame
            movies.loc[movies['movie_id'] == movie_id, list(updated_movie.keys())] = list(updated_movie.values())

            # 保存到CSV
            movies.to_csv('data/movies.csv', index=False)

            # 重新加载数据
            initialize_data()

            # 重定向到编辑后的电影详情页
            return redirect(url_for('admin_movie_detail', movie_id=movie_id))
        except Exception as e:
            return render_template('error.html', message=f"更新电影失败: {str(e)}")

    # 显示编辑表单
    return render_template('admin_edit_movie.html', movie=movie)


@app.route('/admin/delete_movie/<int:movie_id>', methods=['POST'])
def admin_delete_movie(movie_id):
    """
    删除电影路由
    从数据集中移除指定电影及相关评分
    """
    try:
        # 从DataFrame中删除电影
        updated_movies = movies[movies['movie_id'] != movie_id]

        # 从评分数据中删除该电影的评分
        updated_ratings = ratings[ratings['movie_id'] != movie_id]

        # 保存更新后的CSV
        updated_movies.to_csv('data/movies.csv', index=False)
        updated_ratings.to_csv('data/ratings.csv', index=False)

        # 重新加载数据
        initialize_data()

        # 重定向到电影列表
        return redirect(url_for('admin_movies'))
    except Exception as e:
        return render_template('error.html', message=f"删除电影失败: {str(e)}")


# ======================== 用户管理路由 ========================

@app.route('/admin/users')
def admin_users():
    """
    用户管理列表路由
    分页展示所有用户及其统计信息
    """
    if movies is None or ratings is None:
        return render_template('error.html', message="数据加载失败，请检查数据文件")

    # 计算每个用户的评分数量和平均评分
    user_stats = ratings.groupby('user_id').agg(
        rating_count=('rating', 'count'),
        avg_rating=('rating', 'mean')
    ).reset_index()

    # 分页处理
    page = request.args.get('page', 1, type=int)
    per_page = 20
    total_pages = (len(user_stats) + per_page - 1) // per_page
    paginated_users = user_stats.iloc[(page - 1) * per_page: page * per_page].to_dict('records')

    return render_template('admin_users.html',
                           users=paginated_users,
                           current_page=page,
                           total_pages=total_pages)


@app.route('/admin/user/<int:user_id>')
def admin_user_detail(user_id):
    """
    用户详情管理路由
    展示用户的评分历史和详细信息
    """
    if movies is None or ratings is None:
        return render_template('error.html', message="数据加载失败，请检查数据文件")

    # 获取该用户的评分记录
    user_ratings = ratings[ratings['user_id'] == user_id]

    # 合并电影信息
    user_ratings = pd.merge(user_ratings, movies, on='movie_id')

    # 转换为时间格式并按评分日期排序
    user_ratings['rating_date'] = pd.to_datetime(user_ratings['timestamp'], unit='s')
    user_ratings = user_ratings.sort_values('rating_date', ascending=False)

    return render_template('admin_user_detail.html',
                           user_id=user_id,
                           ratings=user_ratings.to_dict('records'))


@app.route('/admin/add_rating', methods=['GET', 'POST'])
def admin_add_rating():
    """
    添加评分路由
    GET：显示添加表单
    POST：处理表单提交并保存新评分
    """
    if movies is None or ratings is None:
        return render_template('error.html', message="数据加载失败，请检查数据文件")

    if request.method == 'POST':
        try:
            # 从表单获取新评分数据
            new_rating = {
                'user_id': int(request.form['user_id']),
                'movie_id': int(request.form['movie_id']),
                'rating': float(request.form['rating']),
                'timestamp': int(datetime.now().timestamp())
            }

            # 添加到DataFrame
            new_row = pd.DataFrame([new_rating])
            updated_ratings = pd.concat([ratings, new_row], ignore_index=True)

            # 保存到CSV
            updated_ratings.to_csv('data/ratings.csv', index=False)

            # 重新加载数据
            initialize_data()

            # 重定向到用户详情页
            return redirect(url_for('admin_user_detail', user_id=new_rating['user_id']))
        except Exception as e:
            return render_template('error.html', message=f"添加评分失败: {str(e)}")

    # 显示添加评分表单
    return render_template('admin_add_rating.html',
                           users=ratings['user_id'].unique(),
                           movies=movies['movie_id'].unique())


@app.route('/admin/delete_rating', methods=['POST'])
def admin_delete_rating():
    """
    删除评分路由
    移除用户对特定电影的评分
    """
    try:
        # 从表单获取用户ID和电影ID
        user_id = int(request.form['user_id'])
        movie_id = int(request.form['movie_id'])

        # 从DataFrame中删除该评分记录
        updated_ratings = ratings[~((ratings['user_id'] == user_id) & (ratings['movie_id'] == movie_id))]

        # 保存到CSV
        updated_ratings.to_csv('data/ratings.csv', index=False)

        # 重新加载数据
        initialize_data()

        # 重定向回用户详情页
        return redirect(url_for('admin_user_detail', user_id=user_id))
    except Exception as e:
        return render_template('error.html', message=f"删除评分失败: {str(e)}")


# ======================== 搜索功能路由 ========================

@app.route('/search')
def search():
    """
    通用搜索路由
    支持按电影或用户搜索
    """
    # 获取查询参数
    query = request.args.get('q', '')
    search_type = request.args.get('type', 'movie')  # 默认搜索电影

    if search_type == 'movie':
        # 电影搜索（按标题）
        results = movies[movies['title'].str.contains(query, case=False, na=False)]
        return render_template('search_results.html',
                               query=query,
                               search_type=search_type,
                               results=results.to_dict('records'),
                               total=len(results))

    elif search_type == 'user':
        # 用户搜索（按ID）
        user_ids = ratings['user_id'].unique()

        try:
            # 尝试将查询转换为用户ID
            user_id = int(query)
            if user_id in user_ids:
                results = [{'user_id': user_id}]
            else:
                results = []
        except ValueError:
            # 查询不是有效数字
            results = []

        return render_template('search_results.html',
                               query=query,
                               search_type=search_type,
                               results=results,
                               total=len(results))

    # 默认返回空结果
    return render_template('search_results.html',
                           query=query,
                           search_type=search_type,
                           results=[],
                           total=0)


@app.route('/search_movies')
def search_movies():
    """
    电影专属搜索路由
    """
    query = request.args.get('q', '')
    results = movies[movies['title'].str.contains(query, case=False, na=False)]
    return render_template('search_movies.html',
                           query=query,
                           results=results.to_dict('records'),
                           total=len(results))


@app.route('/search_users')
def search_users():
    """
    用户专属搜索路由
    """
    query = request.args.get('q', '')
    user_ids = ratings['user_id'].unique()

    try:
        user_id = int(query)
        if user_id in user_ids:
            results = [{'user_id': user_id}]
        else:
            results = []
    except ValueError:
        results = []

    return render_template('search_users.html',
                           query=query,
                           results=results,
                           total=len(results))
@app.route('/user_rated_movies')
def user_rated_movies():
    if not session.get('logged_in') or session.get('role') != 'user':
        return redirect(url_for('login'))

    user_id = session.get('user_id')
    rated_movies = get_user_rated_movies(user_id, ratings, movies)
    return render_template(
        'user_rated_movies.html',
        user_id=user_id,
        rated_movies=rated_movies.to_dict('records'),
        active_tab='rated'
    )
@app.route('/movie_filter', methods=['GET'])
def movie_filter():
    if not session.get('logged_in') or session.get('role') != 'user':
        return redirect(url_for('login'))

    # 获取所有类型、所有年份（用于下拉筛选）
    all_genres = set()
    for gs in movies['genre']:
        if pd.notna(gs):
            all_genres.update([g.strip() for g in gs.split(',')])
    all_genres = sorted(list(all_genres))

    all_years = sorted(movies['year'].dropna().unique(), reverse=True)

    # 获取筛选参数
    genre = request.args.get('genre', '')
    year = request.args.get('year', '')
    min_rating = request.args.get('min_rating', '')
    max_rating = request.args.get('max_rating', '')

    filtered = movies.copy()

    # 筛选逻辑
    if genre:
        filtered = filtered[filtered['genre'].str.contains(genre, na=False)]
    if year:
        filtered = filtered[filtered['year'].astype(str) == str(year)]
    if min_rating:
        filtered = filtered[filtered['imdb_rating'] >= float(min_rating)]
    if max_rating:
        filtered = filtered[filtered['imdb_rating'] <= float(max_rating)]

    # 分页支持
    page = request.args.get('page', 1, type=int)
    per_page = 20
    total_pages = (len(filtered) + per_page - 1) // per_page
    paginated = filtered.iloc[(page-1)*per_page : page*per_page].to_dict('records')

    return render_template(
        'movie_filter.html',
        movies=paginated,
        all_genres=all_genres,
        all_years=all_years,
        total=len(filtered),
        genre=genre,
        year=year,
        min_rating=min_rating,
        max_rating=max_rating,
        current_page=page,
        total_pages=total_pages,
        active_tab='filter'
    )

@app.route('/comments', methods=['GET', 'POST'])
def comments_page():
    from flask import request, session, render_template, redirect, url_for
    movies, _ = load_data()
    comments = load_comments()
    search_title = request.values.get('search_title', '').strip()
    movie_id = request.values.get('movie_id')  # 关键！用于判断是否已选电影
    selected_movie = None
    movie_comments = []
    search_results = []

    if search_title and not movie_id:
        # 搜索但未指定具体电影，返回所有匹配
        filtered = movies[movies['title'].str.contains(search_title, case=False, na=False)]
        if not filtered.empty:
            search_results = filtered.to_dict('records')
        # 页面上只给出可选项，不显示评论区

    if movie_id:
        # 用户已经点选某个具体电影
        selected_movie = movies[movies['movie_id'] == int(movie_id)].iloc[0].to_dict()
        movie_comments = comments[comments['movie_id'] == int(movie_id)] \
            .sort_values('comment_time', ascending=False).to_dict('records')

        # 新增评论逻辑（只有在POST且已选定电影时才允许）
        if request.method == 'POST':
            user_id = session.get('user_id')
            content = request.form.get('content', '').strip()
            if user_id and content:
                comment_id = comments['comment_id'].max() + 1 if not comments.empty else 1
                from datetime import datetime
                new_comment = {
                    'comment_id': comment_id,
                    'movie_id': int(movie_id),
                    'user_id': user_id,
                    'content': content,
                    'comment_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                comments = pd.concat([comments, pd.DataFrame([new_comment])], ignore_index=True)
                save_comments(comments)
                return redirect(url_for('comments_page', search_title=search_title, movie_id=movie_id))

    return render_template(
        'comments.html',
        search_title=search_title,
        search_results=search_results,
        selected_movie=selected_movie,
        comments=movie_comments
    )



@app.route('/delete_comment/<int:comment_id>', methods=['POST'])
def delete_comment(comment_id):
    user_id = session.get('user_id')
    comments = load_comments()
    comment = comments[comments['comment_id'] == comment_id]
    # 注意类型转换！有的csv存成字符串，有的session里是int
    if not comment.empty and (str(comment.iloc[0]['user_id']) == str(user_id)):
        comments = comments[comments['comment_id'] != comment_id]
        save_comments(comments)
        return jsonify({'success': True})
    return jsonify({'success': False, 'msg': '无权删除'})

ALL_ACHIEVEMENTS = [
    {"id": "rate10", "title": "入门影迷", "desc": "已评分10部电影", "icon": "🎬", "color": "#3498db"},
    {"id": "rate50", "title": "电影爱好者", "desc": "已评分50部电影", "icon": "🍿", "color": "#4bc0c0"},
    {"id": "rate100", "title": "资深影迷", "desc": "已评分100部电影", "icon": "🌟", "color": "#f1c40f"},
    {"id": "comment5", "title": "热心观众", "desc": "发布5条评论", "icon": "💬", "color": "#2ecc71"},
    {"id": "comment20", "title": "评论达人", "desc": "发布20条评论", "icon": "🏅", "color": "#9b59b6"},
    {"id": "avg8", "title": "高分玩家", "desc": "评分均分超过8分", "icon": "🔥", "color": "#e67e22"},
    {"id": "early_bird", "title": "早起的鸟儿", "desc": "在早上6点前成功评分1部电影", "icon": "🌅", "color": "#87ceeb"},
    {"id": "night_owl", "title": "夜猫子", "desc": "在晚上11点后成功评分1部电影", "icon": "🌙", "color": "#34495e"},
    {"id": "genre_master", "title": "类型达人", "desc": "评分涉及5种不同电影类型", "icon": "🧩", "color": "#e17055"},
    {"id": "all_rounder", "title": "全能选手", "desc": "评分涉及10种不同电影类型", "icon": "🦸‍♂️", "color": "#00b894"},
    {"id": "marathoner", "title": "连续打卡", "desc": "连续3天每天都评分", "icon": "📅", "color": "#fdcb6e"},
    {"id": "first_comment", "title": "首评", "desc": "发布人生第一条评论", "icon": "✍️", "color": "#d35400"},
    {"id": "legend", "title": "传说中的影评家", "desc": "已评分500部电影", "icon": "🏆", "color": "#e67e22"},
]


def get_user_achievements(user_id):
    import pandas as pd

    achievements = []
    user_ratings = ratings[ratings['user_id'] == user_id]
    user_comments = load_comments()
    user_comments = user_comments[user_comments['user_id'] == user_id]

    # 评分数相关
    if user_ratings.shape[0] >= 10:
        achievements.append("rate10")
    if user_ratings.shape[0] >= 50:
        achievements.append("rate50")
    if user_ratings.shape[0] >= 100:
        achievements.append("rate100")
    if user_ratings.shape[0] >= 500:
        achievements.append("legend")

    # 评论数相关
    if user_comments.shape[0] >= 5:
        achievements.append("comment5")
    if user_comments.shape[0] >= 20:
        achievements.append("comment20")
    if user_comments.shape[0] >= 1:
        achievements.append("first_comment")

    # 高分
    if user_ratings.shape[0] > 0 and user_ratings['rating'].mean() >= 8:
        achievements.append("avg8")

    # 早起和夜猫子
    if user_ratings.shape[0] > 0:
        import datetime
        for t in user_ratings['timestamp']:
            hour = datetime.datetime.fromtimestamp(t).hour
            if hour < 6:
                achievements.append("early_bird")
                break
            if hour >= 23:
                achievements.append("night_owl")
                break

    # 类型成就
    if user_ratings.shape[0] > 0:
        movie_ids = user_ratings['movie_id'].tolist()
        rated_movies = movies[movies['movie_id'].isin(movie_ids)]
        all_genres = set()
        for genre in rated_movies['genre']:
            if pd.notna(genre):
                all_genres.update([g.strip() for g in genre.split(',')])
        if len(all_genres) >= 5:
            achievements.append("genre_master")
        if len(all_genres) >= 10:
            achievements.append("all_rounder")

    # 连续打卡
    if user_ratings.shape[0] > 0:
        import pandas as pd
        days = pd.to_datetime(user_ratings['timestamp'], unit='s').dt.date
        unique_days = sorted(set(days))
        # 检查是否有连续3天
        for i in range(len(unique_days) - 2):
            if (unique_days[i+1] - unique_days[i]).days == 1 and (unique_days[i+2] - unique_days[i+1]).days == 1:
                achievements.append("marathoner")
                break

    return achievements

@app.route('/my_achievements')
def my_achievements():
    if not session.get('logged_in') or session.get('role') != 'user':
        return redirect(url_for('login'))
    user_id = session.get('user_id')
    unlocked = get_user_achievements(user_id)
    # 标记哪些已解锁、哪些待解锁
    for ach in ALL_ACHIEVEMENTS:
        ach['unlocked'] = ach['id'] in unlocked
    return render_template('my_achievements.html', achievements=ALL_ACHIEVEMENTS, active_tab='achievements')
from flask import request, jsonify

import re
import pandas as pd
import requests
from flask import request, jsonify
from utils import load_comments

DEEPSEEK_API_KEY = "sk-037fbadafdd6484daf24cdfd5b2c46ba"
DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"

@app.route('/chatbot_ask', methods=['POST'])
def chatbot_ask():
    msg = request.json.get('msg', '').strip()
    if not msg:
        return jsonify({'reply': '请说点什么吧！'})

    local_answer = None
    try:
        from data_loader import load_data
        movies, ratings = load_data()
        comments = load_comments()

        # 1. 多少部电影
        if re.search(r'多少部电影', msg):
            local_answer = f"目前系统收录了 {len(movies)} 部电影。"

        # 2. 用户X评分了多少部
        m1 = re.search(r'用户\s?(\d+)\s?.*评分了多少', msg)
        if m1:
            uid = int(m1.group(1))
            user_ratings = ratings[ratings['user_id'] == uid]
            n = len(user_ratings)
            if n > 0:
                avg = user_ratings['rating'].mean()
                local_answer = f"用户{uid}共评分 {n} 部电影，平均分 {avg:.2f}。"
            else:
                local_answer = f"用户{uid}还没有评分任何电影哦～"

        # 3. 用户X有哪些评论/评论内容
        m2 = re.search(r'用户\s?(\d+)[\s\S]*?(哪些评论|评论内容|都评论了什么)', msg)
        if m2:
            uid = int(m2.group(1))
            user_comments = comments[comments['user_id'] == uid]
            if not user_comments.empty:
                # 只显示前3条评论摘要，防止太长
                previews = user_comments.head(10)['content'].tolist()
                preview_str = ' | '.join(previews)
                local_answer = f"用户{uid}最近的评论有：{preview_str}。"
            else:
                local_answer = f"用户{uid}还没有发表任何评论哦～"

        # 4. 用户X评论了哪些电影
        m3 = re.search(r'用户\s?(\d+)\s?.*评论过哪些电影|哪些电影被评论', msg)
        if m3:
            uid = int(m3.group(1))
            user_comments = comments[comments['user_id'] == uid]
            movie_ids = user_comments['movie_id'].unique()
            if len(movie_ids) > 0:
                commented_titles = movies[movies['movie_id'].isin(movie_ids)].head(10)['title'].tolist()
                local_answer = f"用户{uid}评论过的电影包括：" + "、".join(commented_titles) + (
                    " 等" if len(movie_ids) > 3 else "") + "。"
            else:
                local_answer = f"用户{uid}还没有评论任何电影哦～"

        # 5. 用户X评分过哪些电影
        m4 = re.search(r'用户\s?(\d+)\s?.*评分过哪些电影', msg)
        if m4:
            uid = int(m4.group(1))
            user_ratings = ratings[ratings['user_id'] == uid]
            movie_ids = user_ratings['movie_id'].unique()
            if len(movie_ids) > 0:
                rated_titles = movies[movies['movie_id'].isin(movie_ids)].head(10)['title'].tolist()
                local_answer = f"用户{uid}评分过的电影包括：" + "、".join(rated_titles) + (
                    " 等" if len(movie_ids) > 3 else "") + "。"
            else:
                local_answer = f"用户{uid}还没有评分过任何电影哦～"

        # 6. 某电影有多少条评论
        m5 = re.search(r'电影(\d+)[^\d]*有(多少|几)条?评论', msg)
        if m5:
            mid = int(m5.group(1))
            n = len(comments[comments['movie_id'] == mid])
            if n > 0:
                title = movies[movies['movie_id'] == mid]['title'].values[0] if not movies[
                    movies['movie_id'] == mid].empty else f'ID为{mid}的电影'
                local_answer = f"《{title}》共有 {n} 条评论。"
            else:
                local_answer = f"电影{mid}还没有评论哦～"

        # 7. 某电影评分多少
        m6 = re.search(r'电影(\d+)[^\d]*评分(多少|几分)?', msg)
        if m6:
            mid = int(m6.group(1))
            movie_ratings = ratings[ratings['movie_id'] == mid]
            if not movie_ratings.empty:
                avg = movie_ratings['rating'].mean()
                title = movies[movies['movie_id'] == mid]['title'].values[0] if not movies[
                    movies['movie_id'] == mid].empty else f'ID为{mid}的电影'
                local_answer = f"《{title}》的平均评分为 {avg:.2f} 分。"
            else:
                local_answer = f"电影{mid}还没有评分数据哦～"

        # 8. 某年评分最高的电影（如“2022年评分最高的3部电影”）
        m7 = re.search(r'(\d{4})年.*评分最高的(\d+)部电影', msg)
        if m7:
            year = m7.group(1)
            top_n = int(m7.group(2))
            movies_year = movies[movies['year'].astype(str) == year]
            if not movies_year.empty:
                avg_ratings = ratings[ratings['movie_id'].isin(movies_year['movie_id'])].groupby('movie_id')[
                    'rating'].mean().reset_index()
                avg_ratings = avg_ratings.sort_values('rating', ascending=False).head(top_n)
                top_titles = movies[movies['movie_id'].isin(avg_ratings['movie_id'])]['title'].tolist()
                local_answer = f"{year}年评分最高的{top_n}部电影有：" + "、".join(top_titles) + "。"
            else:
                local_answer = f"{year}年没有电影数据哦～"

        # 9. 用户最喜欢的类型
        m8 = re.search(r'用户\s?(\d+)\s?.*最喜欢的类型', msg)
        if m8:
            uid = int(m8.group(1))
            user_ratings = ratings[ratings['user_id'] == uid]
            if not user_ratings.empty:
                user_movies = movies[movies['movie_id'].isin(user_ratings['movie_id'])]
                top_genre = user_movies['genre'].value_counts().idxmax()
                local_answer = f"用户{uid}最喜欢的电影类型是：{top_genre}。"
            else:
                local_answer = f"用户{uid}还没有评分过电影，无法分析最喜欢的类型哦～"

        # ...你可以无限扩展，只要pandas能查到的数据都能写进来...
    except Exception as e:
        print("AI助手查本地数据异常：", e)
        local_answer = None

    # RAG部分
    if local_answer:
        system_prompt = (
            "你是电影推荐系统的AI助手。"
            "每次我会给你用户原始问题，以及根据数据库查到的事实。"
            "请你用自然口吻，把事实融入对用户的友好回答中。"
        )
        user_prompt = (
            f"用户提问：{msg}\n"
            f"系统查到的数据：{local_answer}\n"
            "请结合两者自然作答（如有评价可适当加鼓励/引导）。"
        )
    else:
        system_prompt = (
            "你是电影推荐系统的AI助手，请尽量用自然语言回答电影相关问题。"
        )
        user_prompt = msg

    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.4,
        "max_tokens": 256
    }
    try:
        resp = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload, timeout=20)
        resp.raise_for_status()
        result = resp.json()
        reply = result['choices'][0]['message']['content'].strip()
        return jsonify({'reply': reply})
    except Exception as e:
        print("DeepSeek API error:", e)
        return jsonify({'reply': '抱歉，AI接口出错了，请稍后再试～'})

@app.route('/profile')
def user_profile():
    if not session.get('logged_in') or session.get('role') != 'user':
        return redirect(url_for('login'))
    user_id = session.get('user_id')

    # 获取评分数据
    rated_movies = get_user_rated_movies(user_id, ratings, movies)
    rated_count = len(rated_movies)
    avg_rating = round(rated_movies['rating'].mean(), 2) if rated_count > 0 else "—"

    # 获取评论数据
    user_comments = load_comments()
    user_comments = user_comments[user_comments['user_id'] == user_id]

    # 获取成就
    unlocked = get_user_achievements(user_id)

    # 统计最喜欢类型
    if rated_count > 0:
        genre_list = []
        for g in rated_movies['genre']:
            if pd.notna(g):
                genre_list += [i.strip() for i in g.split(',')]
        if genre_list:
            from collections import Counter
            fav_genre = Counter(genre_list).most_common(1)[0][0]
        else:
            fav_genre = "无"
    else:
        fav_genre = "无"

    # 注册/活跃天数（以首次评分为注册日）
    if rated_count > 0:
        join_date = pd.to_datetime(rated_movies['rating_date']).min().date()
    else:
        join_date = None
    from datetime import date
    days_active = (date.today() - join_date).days if join_date else 0

    # 高分电影
    if rated_count > 0:
        best_movie = rated_movies.loc[rated_movies['rating'].idxmax()].to_dict()
    else:
        best_movie = None

    return render_template(
        'profile.html',
        user_id=user_id,
        rated_count=rated_count,
        avg_rating=avg_rating,
        comment_count=len(user_comments),
        achievements=ALL_ACHIEVEMENTS,
        unlocked=unlocked,
        rated_movies=rated_movies.to_dict('records'),
        comments=user_comments.to_dict('records'),
        fav_genre=fav_genre,
        join_date=join_date,
        days_active=days_active,
        best_movie=best_movie
    )
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import pandas as pd
import jieba
import os

@app.route('/comments_wordcloud')
def comments_wordcloud():
    comments = pd.read_csv('data/comments.csv')
    text = ' '.join(str(x) for x in comments['content'] if pd.notnull(x))
    text = ' '.join(jieba.cut(text))  # 中文分词

    # 字体路径适配（自动找本地可用字体）
    font_path = 'SimHei.ttf'
    if not os.path.exists(font_path):
        font_path = 'C:/Windows/Fonts/simhei.ttf'
    if not os.path.exists(font_path):
        font_path = 'C:/Windows/Fonts/msyh.ttc'
    if not os.path.exists(font_path):
        font_path = '/usr/share/fonts/truetype/wqy/wqy-microhei.ttc'
    if not os.path.exists(font_path):
        font_path = None   # 英文环境下可以不传

    wc = WordCloud(
        font_path=font_path,
        background_color='white',
        width=600, height=300, max_words=100
    ).generate(text)

    plt.figure(figsize=(8,4))
    plt.imshow(wc, interpolation='bilinear')
    plt.axis('off')
    plt.tight_layout(pad=0)
    from io import BytesIO
    buf = BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', pad_inches=0.1)
    plt.close()
    buf.seek(0)
    return app.response_class(buf.read(), mimetype='image/png')

@app.route('/admin/comments')
def admin_comments():
    if not session.get('logged_in') or session.get('role') != 'admin':
        return redirect(url_for('login'))
    comments = load_comments()
    # 可选：支持简单搜索
    keyword = request.args.get('q', '').strip()
    if keyword:
        # 按内容或电影id模糊查找
        mask = comments['content'].str.contains(keyword, na=False)
        try:
            movie_id = int(keyword)
            mask |= (comments['movie_id'] == movie_id)
        except:
            pass
        comments = comments[mask]
    # 按时间倒序
    comments = comments.sort_values('comment_time', ascending=False)
    # 分页
    page = request.args.get('page', 1, type=int)
    per_page = 20
    total = len(comments)
    total_pages = (total + per_page - 1) // per_page
    comments = comments.iloc[(page-1)*per_page : page*per_page]
    # 关联电影标题（可选，提升体验）
    movie_map = {row['movie_id']: row['title'] for _, row in movies.iterrows()}
    return render_template(
        'admin_comments.html',
        comments=comments.to_dict('records'),
        keyword=keyword,
        total=total,
        total_pages=total_pages,
        current_page=page,
        movie_map=movie_map,
    )
@app.route('/admin/delete_comment/<int:comment_id>', methods=['POST'])
def admin_delete_comment(comment_id):
    if not session.get('logged_in') or session.get('role') != 'admin':
        return jsonify({'success': False, 'msg': '无权操作'})
    comments = load_comments()
    if comment_id not in comments['comment_id'].values:
        return jsonify({'success': False, 'msg': '评论不存在'})
    # 删除
    comments = comments[comments['comment_id'] != comment_id]
    save_comments(comments)
    return jsonify({'success': True})

@app.route('/messages', methods=['GET', 'POST'])
def user_messages():
    if not session.get('logged_in') or session.get('role') != 'user':
        return redirect(url_for('login'))
    user_id = session['user_id']
    feedback = load_feedback()

    # 发送新反馈
    if request.method == 'POST':
        content = request.form.get('content', '').strip()
        if content:
            fid = feedback['id'].max() + 1 if not feedback.empty else 1
            new_msg = {
                'id': fid,
                'sender_id': user_id,
                'receiver_id': 'admin',
                'content': content,
                'create_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'is_read': 0,
                'type': 'feedback'
            }
            feedback = pd.concat([feedback, pd.DataFrame([new_msg])], ignore_index=True)
            save_feedback(feedback)
            return redirect(url_for('user_messages'))

    # 只看与自己有关的消息（收发都算）
    inbox = feedback[(feedback['receiver_id'].astype(str)==str(user_id)) | (feedback['sender_id'].astype(str)==str(user_id))]
    # 按时间倒序
    inbox = inbox.sort_values('create_time', ascending=False)
    # 标记所有收到消息为已读
    unread_mask = (inbox['receiver_id'].astype(str) == str(user_id)) & (inbox['is_read'] == 0)
    if unread_mask.any():
        feedback.loc[inbox[unread_mask].index, 'is_read'] = 1
        save_feedback(feedback)
    return render_template('messages.html', inbox=inbox.to_dict('records'))

@app.route('/admin/feedback', methods=['GET', 'POST'])
def admin_feedback():
    if not session.get('logged_in') or session.get('role') != 'admin':
        return redirect(url_for('login'))
    feedback = load_feedback()

    # 管理员主动发送消息
    if request.method == 'POST':
        user_id = request.form.get('user_id')
        content = request.form.get('content', '').strip()
        if user_id and content:
            fid = feedback['id'].max() + 1 if not feedback.empty else 1
            new_msg = {
                'id': fid,
                'sender_id': 'admin',
                'receiver_id': user_id,
                'content': content,
                'create_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'is_read': 0,
                'type': 'notice'
            }
            feedback = pd.concat([feedback, pd.DataFrame([new_msg])], ignore_index=True)
            save_feedback(feedback)
            return redirect(url_for('admin_feedback'))

    # 自动将所有发给管理员且未读的消息，设为已读
    mask = (feedback['receiver_id'] == 'admin') & (feedback['is_read'] == 0)
    if mask.any():
        feedback.loc[mask, 'is_read'] = 1
        save_feedback(feedback)

    inbox = feedback.sort_values('create_time', ascending=False)
    return render_template('admin_feedback.html', inbox=inbox.to_dict('records'))

# ======================== 程序入口 ========================

if __name__ == '__main__':
    app.run(debug=True)  # 启动Flask应用
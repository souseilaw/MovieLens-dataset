import pandas as pd
import sqlite3 as db
import sys, heapq, logging, os
from sqlite3 import Error as DbError
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

if len(sys.argv) < 2:
    print("Please specify path to folder that contains movielen's 20M dataset")
    sys.exit(0)
path_to_csv = sys.argv[1]
movies_csv = path_to_csv + '/movies.csv'
ratings_csv = path_to_csv + '.ratings.csv'
links_csv = path_to_csv + '/links.csv'
genome_scores_csv = path_to_csv + '/genome-scores.csv'
genome_tags_csv = path_to_csv + '/genome-tags.csv'
db_path = path_to_csv + '/test.db'

con = None
try:
    con = db.connect(db_path)
except DbError as e:
    #TODO: clean is up required after every exception
    print(e)
    sys.exit(0)
cursor = con.cursor()

def bootstrap_db(cursor):
    df_movies = pd.read_csv(movies_csv)
    df_ratings = pd.read_csv("ratings.csv")
    new_movies_column = ["Adventure", "Animation", "Children", "Comedy", "Fantasy", "Horror", "Drama", "Thriller", "Romance"]
    for col in new_movies_column:
        df_movies[col] = 1
    df_movies["year"] = 1
    for index, row in df_movies.iterrows():
        df_movies.loc[index, "year"] = row['title'].split(" ")[-1][1:-1]
    for index, row in df_movies.iterrows():
        for col in new_movies_column:
            if col in row['genres']:
                df_movies.loc[index, col] = 1
            else:
                df_movies.loc[index, col] = 0       
    df_links = pd.read_csv(links_csv)
    #join links and movies to avoid join later on
    df_movies = pd.merge(df_movies, df_links, on = 'movieId', how = 'inner')
    df_genome_scores = pd.read_csv(genome_scores_csv)
    df_genome_tags = pd.read_csv(genome_tags_csv)
    #join genome score and tags
    df_genome_scores = pd.merge(df_genome_scores, df_genome_tags, on = 'tagId', how = 'inner')

    table_to_df ={'movies':df_movies, 'genome_score': df_genome_scores}
    cursor.execute('DROP TABLE IF EXISTS ratings')
    cursor.execute('DROP TABLE IF EXISTS aggregated_ratings')
    con.commit()
    
    rating_year = []
    for index,row in df_ratings.iterrows():
        year = datetime.fromtimestamp(row['timestamp']).year
        year = year - (year % 5)
        rating_year.append(year)

    df_ratings['rating_year'] = rating_year
    start = 0
    for i in range(1,int(df_ratings.shape[0]/500000)): 
        df_ratings[start:start+500000].to_sql(con=con, if_exists="append", name="ratings") 
        start += 500000
    for table_name, df in table_to_df.items():
        df.to_sql(name=table_name, con = con, if_exists='replace')
    cursor.execute('''create table aggregated_ratings as select count(movieId) as row_count, 
    avg(rating) as average_rating, movieId from ratings group by movieId''')
    cursor.execute("CREATE INDEX movieAggrRatingIndex on aggregated_ratings(movieId)")
    cursor.execute("CREATE INDEX movieRatingIndex on ratings(movieId)")
    cursor.execute("CREATE INDEX movieIndex on movies(movieId)")
    con.commit()

def get_popular_movies(cursor):
    logging.info("Getting top 10 popular movies")
    cursor.execute("SELECT row_count, average_rating, movieId FROM aggregated_ratings")
    rows = cursor.fetchall()
    cursor.execute("select max(row_count) as max_count from (select row_count, movieId from aggregated_ratings)")
    N = cursor.fetchall()[0][0]
    cursor.execute("select avg(rating) from ratings")
    global_average = cursor.fetchall()[0][0]
    score = {}
    #calculate baysian average of ratings
    for row in rows:
        row_count = row[0]
        row_average = row[1]
        row_id = row[2]
        weight = (row_count/N)
        score[row_id] = (weight * row_average) + (1 - weight) * global_average
    popular_movies_tuple = heapq.nlargest(10, score.items(), key=lambda item: (item[1], item[0]))
    movieIds = [r[0] for r in popular_movies_tuple]
    sql = "SELECT movieId, title FROM movies where movieId in {}".format(str(tuple(movieIds)))
    cursor.execute(sql)
    movie_names = {}
    for r in cursor.fetchall():
        movie_names[r[0]] = r[1][0:-6]
    X = [movie_names[r[0]] for r in popular_movies_tuple]
    Y = [r[1] for r in popular_movies_tuple]
    index = np.arange(len(X))
    plt.bar(index, Y)
    plt.xlabel('Movies')
    plt.ylabel('Weighted Average Score')
    plt.xticks(index, X,rotation=270, fontsize=6)
    plt.title("Top 10 popular movies of all time")
    plt.tight_layout()
    plt.show()    

def get_movies_with_most_5ratings(cursor):
    logging.info("Getting movies with most number of 5 star")
    cursor.execute('''SELECT movies.movieId, A.count, movies.title from 
    (SELECT count(*) as count,movieId FROM ratings where rating = '5.0' GROUP BY 
    movieId ORDER BY count DESC LIMIT 10) A, movies where movies.movieId = A.movieId''')
    rows = cursor.fetchall()
    return rows

def get_pop_movies_adventure(cursor):
    cursor.execute(''' select title, A.count as rating from movies, 
    (select count(*) as count,movieId from ratings where rating = 5 group by movieId order by count DESC LIMIT 100) A 
    where movies.adventure = 1 and movies.movieId = A.movieId ORDER BY rating DESC LIMIT 10''')
    X = []
    Y = []
    for row in cursor.fetchall():
        X.append(row[0][0:15])
        Y.append(row[1])
    index = np.arange(len(X))
    plt.bar(index, Y)
    plt.xlabel('Movie')
    plt.ylabel('Number of 5 stars')
    plt.xticks(index, X, fontsize=5)
    plt.title("Most 5 star rated movies in Adventure category")
    plt.show()

def get_average_ratings_categories(cursor):
    ratings = []
    columns = ["Adventure", "Animation", "Children", "Comedy", "Fantasy", "Horror", "Drama", "Thriller", "Romance"]
    for col in columns: 
        sql = '''select avg(average_rating) from aggregated_ratings where movieId in (select movieId from movies where ''' + col + ''' = 1)''' 
        cursor.execute(sql)
        ratings.append(cursor.fetchall()[0][0])
    index = np.arange(len(columns))
    plt.bar(index, ratings)
    plt.xlabel('Genre')
    plt.ylabel('Average Ratings')
    plt.xticks(index, columns, fontsize=7)
    plt.title("All time popularity of genres")
    plt.show()

def get_popularity_over_time(cursor):
    movies = get_movies_with_most_5ratings(cursor)
    movie_ids = [(r[0], r[2]) for r in movies]
    movie_ratings = {}
    for movie in movie_ids:
        sql = '''select avg(rating), rating_year from ratings where movieId = ''' + str(movie[0]) + ''' group by rating_year order by rating_year;'''
        cursor.execute(sql)
        rows = cursor.fetchall()
        movie_ratings[movie[0]] = {'y':[r[0] for r in rows], 'x': [r[1] for r in rows], 'title': movie[1]}
    fig = plt.figure()
    ax = plt.subplot(111)
    for key, movie in movie_ratings.items():
        ax.plot(movie['x'], movie['y'], label=movie['title'])
    plt.title('Popularity of top 10 movies over time')
    plt.xticks([1995, 2000, 2005, 2010, 2015])
    # Shrink current axis by 20%
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    # Put a legend to the right of the current axis
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    #ax.legend()
    plt.show()

def get_popularity_genre_over_time(cursor):
    columns = ["Adventure", "Animation", "Children", "Comedy", "Fantasy", "Horror", "Drama"]
    ratings = {}
    for col in columns:
        sql = ''' select avg(rating), rating_year from ratings where movieId in (select movieId from movies where ''' + col + ''' = 1) group by rating_year;'''
        cursor.execute(sql)
        rows = cursor.fetchall()
        ratings[col] = {'x': [r[1] for r in rows], 'y': [r[0] for r in rows]}
    
    fig = plt.figure()
    ax = plt.subplot(111)
    for col, r_value in ratings.items():
        ax.plot(r_value['x'], r_value['y'], label=col)
    
    plt.title('Popularity of genres over time')
    plt.xticks([1995, 2000, 2005, 2010, 2015])
    # Shrink current axis by 20%
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    # Put a legend to the right of the current axis
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    #ax.legend()
    plt.show()

def get_movies_per_decade(cursor):
    cursor.execute('''Select year,count(*) as count from movies where year != '' and year > '1899' and year not like ('%-%') 
    and year not like ('%a%') and year not like ('%e') and year not like ('%i%') and year not 
    like ('%o%') and year not like ('%u%') and year not like ('%k%') and year not like ('%y%') and 
    year not like ('%w%') and year not like ('%)') group by year order by year asc''') 
    result = cursor.fetchall()
    i=0
    year_count = []
    year_list = []
    yearCount = 0
    years = " "
    for row in result :
        i=i+1
        if i == 11 :
            i = 1
            years = " "
            yearCount = 0
        if i == 1 :
            years = years + row[0]
            yearCount = yearCount + row[1]
        if i < 10 : 
            yearCount = yearCount + row[1]
        if i == 10 :    
            years = years + "-" + row[0]
            yearCount = yearCount + row[1]
            year_list.append(years)
            year_count.append(yearCount)   
    
    plt.rc('xtick',labelsize=5)
    plt.rc('ytick',labelsize=5)
    plt.xticks(
    rotation=45, 
    fontweight='light',
    fontsize=8  
    )
    
    plt.bar(year_list,year_count,color=(0.1, 0.1, 0.1, 0.1),  edgecolor='blue')  
    plt.show()

bootstrap_db(cursor)
get_popular_movies(cursor)
get_pop_movies_adventure(cursor)
get_average_ratings_categories(cursor)
get_popularity_over_time(cursor)
get_popularity_genre_over_time(cursor)
get_movies_per_decade(cursor)
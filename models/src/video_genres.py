
def model(dbt, session):
    raw_df = dbt.ref('std_movie_video_genres').df()
    video_genres_df = raw_df[['video_id', 'genre_id']]
    return video_genres_df

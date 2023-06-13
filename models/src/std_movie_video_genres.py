import pandas as pd


def model(dbt, session):
    raw_df = dbt.source('std', 'title.basics').df()
    genre = raw_df['genres'].str.split(',', n=-1, expand=True)
    video_genres = pd.concat([raw_df['tconst'], genre], axis=1)
    video_genres = pd.melt(video_genres, id_vars=['tconst'], value_name='name').dropna().drop(['variable'], axis=1)

    all_genres_df = dbt.ref('std_genres').df()
    video_genres = pd.merge(video_genres, all_genres_df)
    video_genres.drop(['name'], axis=1, inplace=True)
    video_genres.sort_values(by=['tconst'], ignore_index=True, inplace=True)
    video_genres.rename(columns={'tconst': 'video_id', 'id': 'genre_id'}, inplace=True)

    video_genres.reset_index(drop=True, inplace=True)
    video_genres.insert(0, 'id', video_genres.index)
    video_genres['id'] = video_genres['id'].astype('str')

    return video_genres

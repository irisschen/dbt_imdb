import pandas as pd


def model(dbt, session):
    persons = dbt.ref('reformat_persons').df()
    persons.drop(['name'], axis=1, inplace=True)
    video_persons = dbt.ref('video_persons').df()
    video_persons = video_persons.merge(persons, left_on='person_id', right_on='id', how='left')
    video_persons.drop(['id', 'person_id'], axis=1, inplace=True)

    video_genres = dbt.ref('video_genres').df()
    genres = dbt.ref('genres').df()
    video_genres = video_genres.merge(genres, left_on='genre_id', right_on='id', how='left')
    video_genres.drop(['id', 'genre_id'], axis=1, inplace=True)

    video_persons.rename(columns={'video_id': 'head_name', 'reformat': 'tail_name'}, inplace=True)
    video_persons.insert(1, 'tail_type', 'film.artists')
    video_genres.rename(columns={'video_id': 'head_name', 'name': 'tail_name'}, inplace=True)
    video_genres.insert(1, 'tail_type', 'film.genres')
    triplets = pd.concat([video_genres, video_persons], ignore_index=True, )

    return triplets

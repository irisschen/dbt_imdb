import pandas as pd


def model(dbt, session):
    raw_df = dbt.source('std', 'title.basics').df()
    genres_df = pd.DataFrame(columns=['name'])
    genres_df['name'] = raw_df['genres'].str.split(',')
    genres_df = genres_df.explode(['name'])
    genres_df = genres_df[genres_df['name'] != '\\N']
    genres_df.drop_duplicates(inplace=True, ignore_index=True)
    genres_df.reset_index(drop=True, inplace=True)
    genres_df.insert(0, 'id', genres_df.index)
    genres_df['id'] = genres_df['id'].astype('str')
    return genres_df


def model(dbt, session):
    raw_df = dbt.source('std', 'title.basics').df()
    rating_df = raw_df[['tconst', 'isAdult']]
    rating_df.rename(columns={'tconst': 'video_id', 'isAdult': 'content_rating_id'}, inplace=True)
    rating_df.replace({0: 'non-adult', 1: 'adult'}, inplace=True)

    rating_df.reset_index(drop=True, inplace=True)
    rating_df.insert(0, 'id', rating_df.index)
    rating_df['id'] = rating_df['id'].astype('str')

    return rating_df

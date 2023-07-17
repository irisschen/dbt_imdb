
def model(dbt, session):
    args = dbt.config.get('content_rating')
    raw_df = dbt.ref('std_movie_video_content_ratings').df()

    video_df = raw_df[~raw_df['content_rating_id'].isin(args)][['video_id']]
    video_df.drop_duplicates(keep='first', inplace=True)
    return video_df

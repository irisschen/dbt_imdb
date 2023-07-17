
def model(dbt, session):
    raw_df = dbt.ref('std_movie_video_persons').df()
    video_persons_df = raw_df[['video_id', 'person_id']]
    video_df = dbt.ref('videos').df()['video_id']
    target = video_persons_df[video_persons_df['video_id'].isin(video_df)][['video_id', 'person_id']]
    return target

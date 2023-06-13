import pandas as pd


def model(dbt, session):
    raw_df = dbt.source('std', 'title.crew').df()
    header = raw_df.iloc[0]
    raw_df = raw_df[1:]
    raw_df.columns = header

    video_persons = pd.melt(raw_df, id_vars=['tconst'],
                            value_name='person_id', var_name='role').sort_values('tconst')
    video_persons = video_persons[video_persons['person_id'] != '\\N']

    video_persons['person_id'] = video_persons['person_id'].str.split(',')
    video_persons = video_persons.explode(['person_id'])
    video_persons.drop_duplicates(inplace=True)
    video_persons.rename(columns={'tconst': 'video_id'}, inplace=True)
    video_persons.reset_index(drop=True, inplace=True)

    video_persons = video_persons[['video_id', 'person_id', 'role']]

    video_persons.insert(0, 'id', video_persons.index)
    video_persons['id'] = video_persons['id'].astype('str')

    return video_persons

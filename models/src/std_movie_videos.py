import pandas as pd
import datetime


def model(dbt, session):
    raw_df = dbt.source('std', 'title.basics').df()
    videos = pd.DataFrame()

    videos['id'] = raw_df['tconst'].str.strip()
    videos['name'] = raw_df['primaryTitle'].str.strip()
    videos['type'] = raw_df['titleType'].str.strip()
    videos['date_published'] = raw_df['startYear'].map(
        lambda x: datetime.datetime.strptime(x, "%Y") if x != '\\N' else None)
    videos['start_date'] = raw_df['startYear'].map(
        lambda x: datetime.datetime.strptime(x, "%Y") if x != '\\N' else None)
    videos['end_date'] = raw_df['endYear'].map(
        lambda x: datetime.datetime.strptime(x, "%Y") if x != '\\N' else None)
    videos['description'] = None
    videos['description'] = videos['description'].astype(str)

    videos['duration'] = raw_df['runtimeMinutes'].map(
        lambda x: int(x)*60 if x != '\\N' else None).astype(pd.Int64Dtype())
    return videos

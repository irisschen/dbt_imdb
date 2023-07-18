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


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {"std.title.basics": "read_csv_auto(\u0027data/title.basics.tsv.gz\u0027, sample_size=-1, quote=\"\\t\")"}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "imdb"
    schema = "main"
    identifier = "std_movie_videos"
    
    def __repr__(self):
        return '"imdb"."main"."std_movie_videos"'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------



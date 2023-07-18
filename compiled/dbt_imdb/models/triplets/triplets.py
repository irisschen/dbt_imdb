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


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"genres": "\"imdb\".\"main\".\"genres\"", "reformat_persons": "\"imdb\".\"main\".\"reformat_persons\"", "video_genres": "\"imdb\".\"main\".\"video_genres\"", "video_persons": "\"imdb\".\"main\".\"video_persons\""}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
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
    identifier = "triplets"
    
    def __repr__(self):
        return '"imdb"."main"."triplets"'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------



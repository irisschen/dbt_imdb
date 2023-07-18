def model(dbt, session):
    raw_df = dbt.ref('std_movie_video_persons').df()
    video_persons_df = raw_df[['video_id', 'person_id']]
    video_df = dbt.ref('videos').df()['video_id']
    target = video_persons_df[video_persons_df['video_id'].isin(video_df)][['video_id', 'person_id']]
    return target


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"std_movie_video_persons": "\"imdb\".\"main\".\"std_movie_video_persons\"", "videos": "\"imdb\".\"main\".\"videos\""}
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
    identifier = "video_persons"
    
    def __repr__(self):
        return '"imdb"."main"."video_persons"'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------



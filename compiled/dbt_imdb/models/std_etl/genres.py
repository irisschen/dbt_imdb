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
    sources = {"std.title.basics": "read_csv_auto(\u0027data/title.basics.tsv\u0027, sample_size=-1, quote=\"\\t\")"}
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
    identifier = "genres"
    
    def __repr__(self):
        return '"imdb"."main"."genres"'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------



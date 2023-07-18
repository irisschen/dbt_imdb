
  
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
    sources = {"std.title.crew": "read_csv_auto(\u0027data/title.crew.tsv\u0027, sample_size=-1, quote=\"\\t\")"}
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
    identifier = "std_movie_video_persons"
    
    def __repr__(self):
        return '"imdb"."main"."std_movie_video_persons"'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------




def materialize(df, con):
    try:
        import pyarrow
        pyarrow_available = True
    except ImportError:
        pyarrow_available = False
    finally:
        if pyarrow_available and isinstance(df, pyarrow.Table):
            # https://github.com/duckdb/duckdb/issues/6584
            import pyarrow.dataset
    con.execute('create table "imdb"."main"."std_movie_video_persons__dbt_tmp" as select * from df')

  
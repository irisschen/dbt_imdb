
  
    def model(dbt, session):
    raw_df = dbt.source('std', 'title.basics').df()
    rating_df = raw_df[['tconst', 'isAdult']]
    rating_df.rename(columns={'tconst': 'video_id', 'isAdult': 'content_rating_id'}, inplace=True)
    rating_df.replace({0: 'non-adult', 1: 'adult'}, inplace=True)

    rating_df.reset_index(drop=True, inplace=True)
    rating_df.insert(0, 'id', rating_df.index)
    rating_df['id'] = rating_df['id'].astype('str')

    return rating_df


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
    identifier = "std_movie_video_content_ratings"
    
    def __repr__(self):
        return '"imdb"."main"."std_movie_video_content_ratings"'


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
    con.execute('create table "imdb"."main"."std_movie_video_content_ratings__dbt_tmp" as select * from df')

  
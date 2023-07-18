
  
    import pandas as pd


def model(dbt, session):
    raw_df = dbt.source('std', 'title.basics').df()
    genre = raw_df['genres'].str.split(',', n=-1, expand=True)
    video_genres = pd.concat([raw_df['tconst'], genre], axis=1)
    video_genres = pd.melt(video_genres, id_vars=['tconst'], value_name='name').dropna().drop(['variable'], axis=1)

    all_genres_df = dbt.ref('genres').df()
    video_genres = pd.merge(video_genres, all_genres_df)
    video_genres.drop(['name'], axis=1, inplace=True)
    video_genres.sort_values(by=['tconst'], ignore_index=True, inplace=True)
    video_genres.rename(columns={'tconst': 'video_id', 'id': 'genre_id'}, inplace=True)

    video_genres.reset_index(drop=True, inplace=True)
    video_genres.insert(0, 'id', video_genres.index)
    video_genres['id'] = video_genres['id'].astype('str')

    return video_genres


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"genres": "\"imdb\".\"main\".\"genres\""}
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
    identifier = "movie_video_genres"
    
    def __repr__(self):
        return '"imdb"."main"."movie_video_genres"'


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
    con.execute('create table "imdb"."main"."movie_video_genres__dbt_tmp" as select * from df')

  
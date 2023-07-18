import string


def model(dbt, session):
    metadata_df = dbt.ref('persons').df()
    args = dbt.config.get('not_run_models')
    if str(dbt.this).split('.')[-1][1:-1] in args:
        metadata_df['reformat'] = metadata_df['name']
        target = metadata_df
    else:
        metadata_df['reformat'] = metadata_df['name'].map(reformat)
        target = metadata_df
    return target


def reformat(artist):
    # remove white space
    artist = artist.translate(str.maketrans('', '', string.whitespace))

    # remove [***]
    try:
        start = artist.index('[')
        end = artist.index(']')+1
        artist = artist.replace(artist[start:end], '')
    except ValueError:
        pass

    # remove punctuation
    artist = artist.translate(str.maketrans('', '', string.punctuation))
    punctuation = '・＝、★℃☆―×‘’“”∴〜『』♂【】（）「」［］！％'
    artist = artist.translate(str.maketrans('', '', punctuation))

    # remove numbers
    artist = artist.translate(str.maketrans('', '', string.digits))

    # lower case
    artist = artist.lower()

    return artist


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"persons": "\"imdb\".\"main\".\"persons\""}
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


config_dict = {'not_run_models': []}


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
    identifier = "reformat_persons"
    
    def __repr__(self):
        return '"imdb"."main"."reformat_persons"'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------



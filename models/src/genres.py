
def model(dbt, session):
    raw_df = dbt.ref('std_genres').df()
    genres_df = raw_df[['id', 'name']]
    return genres_df

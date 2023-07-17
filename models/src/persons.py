
def model(dbt, session):
    raw_df = dbt.ref('std_persons').df()
    persons_df = raw_df[['id', 'name']]
    return persons_df

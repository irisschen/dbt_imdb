
def model(dbt, session):
    raw_df = dbt.source('std', 'title.akas').df()
    names = raw_df[['titleId', 'title']]
    names.rename({'titleId': 'video_id', 'title': 'name'}, inplace=True)
    names.drop_duplicates(inplace=True, ignore_index=True)

    names.reset_index(drop=True, inplace=True)
    names.insert(0, 'id', names.index)
    names['id'] = names['id'].astype('str')

    return names

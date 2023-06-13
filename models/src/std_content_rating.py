import pandas as pd


def model(dbt, session):
    content_rating = pd.DataFrame(['adult', 'non-adult'], columns=['id'])
    return content_rating

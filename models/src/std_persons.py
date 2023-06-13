import pandas as pd
import datetime


def model(dbt, session):
    raw_df = dbt.source('std', 'name.basics').df()
    header = raw_df.iloc[0]
    raw_df = raw_df[1:]
    raw_df.columns = header
    persons = pd.DataFrame()

    persons['id'] = raw_df['nconst'].str.strip()
    persons['name'] = raw_df['primaryName'].str.strip()
    persons['birth_date'] = raw_df['birthYear'].map(
        lambda x: datetime.datetime.strptime(x, "%Y") if x != '\\N' else None)

    persons['birth_date'] = raw_df['birthYear'].map(
        lambda x: int(x) * 1e9 if x != '\\N' else None)

    persons['nationality'] = None
    persons['nationality'] = persons['nationality'].astype(str)
    return persons

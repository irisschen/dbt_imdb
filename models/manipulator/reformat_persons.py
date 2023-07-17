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

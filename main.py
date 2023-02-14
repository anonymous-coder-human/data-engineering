from urllib.error import URLError
import pandas as pd
import io
import requests


# Here are the important URLS at the top, so they can easily be changed
CONS_URL = "https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv"
CONS_EMAIL_URL = "https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv"
CONS_SUB_URL = "https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/" \
               "cons_email_chapter_subscription.csv"


def data_downloader(url: str) -> pd.DataFrame:
    """
    This data downloader offers a few benefits to simply calling pd.read_csv on its own
    The first is that some ssl features are not supported in certain versions of pandas that
    are supported in requests, so offering that as a fallback expands reliability. Second,
    if all else fails, being able to see which URL is not working in the error message will help debugging

    Args:
        url (str): The url to gather data from
    Return:
        object: The dataframe from that url
    """
    try:
        return pd.read_csv(url)
    except URLError:
        try:
            s = requests.get(url).content
            return pd.read_csv(io.StringIO(s.decode('utf-8')))
        except Exception as e:
            print(f'When fetching {url} caught {type(e)}: e')


def extract_cons_data(cons_url: str, cons_email_url: str, cons_sub_url: str) -> tuple:
    """
    This simply routes the three urls into three downloaded dataframes using the data downloader.
    The order is always cons, cons_email, cons_sub.

    Args:
        cons_url (str): The url to the cons data (unique id "cons_id")
        cons_email_url (str): The url to the cons email data (unique id "cons_email_id")
        cons_sub_url (str): The url to the cons subscription data
    Return
        object: cons, cons_email, cons_sub
    """
    cons = data_downloader(cons_url)
    cons_email = data_downloader(cons_email_url)
    cons_sub = data_downloader(cons_sub_url)
    return cons, cons_email, cons_sub


def transform_cons_data(cons: pd.DataFrame, cons_email: pd.DataFrame, cons_sub: pd.DataFrame) -> pd.DataFrame:
    """
    This function starts by filtering cons_sub by 'chapter_id' == 1.
    It continues by filtering cons_email by 'is_primary' == 1.
    Then we one_to_one merge cons to cons_email by 'cons_id'.
    Then we one_to_one merge cons_all to cons_sub by 'cons_email_id'.
    After that there is basic renaming, imputing NaN is_unsub to False, and selecting columns

    Args:
        cons (object): the cons dataframe (unique id "cons_id")
        cons_email (object): the cons email dataframe (unique id "cons_email_id")
        cons_sub (object): cons subscription dataframe
    Return
        object: Dataframe with the columns - email, code, is_unsub, created_dt, updated_dt
    """
    cons_sub = cons_sub[cons_sub['chapter_id'] == 1]
    cons_email = cons_email[cons_email['is_primary'] == 1]
    cons_all = cons.merge(cons_email, on="cons_id", validate="one_to_one", suffixes=("", "_email"))
    cons_all = cons_all.merge(cons_sub, on="cons_email_id", validate="one_to_one", suffixes=("", "_sub"))
    cons_all.rename(columns={'create_dt': 'created_dt_orig'}, inplace=True)
    cons_all.rename(columns={'source': 'code',
                             'isunsub': 'is_unsub',
                             'created_dt_orig': 'created_dt',
                             'modified_dt': 'updated_dt'
                             }, inplace=True)
    cons_all['is_unsub'] = cons_all['is_unsub'].fillna(False)
    cons_all['is_unsub'] = cons_all['is_unsub'].astype(bool)
    people = cons_all[["email", "code", "is_unsub", 'created_dt', 'updated_dt']]
    return people


def aggregate_cons_data(transformed_cons: pd.DataFrame) -> pd.DataFrame:
    """
    This function takes the created_dt column in the input dataframe,
    converts it to a date, groups by that date, counts the results, and formats the dataframe appropriately

    Args:
        transformed_cons (object): this should be a dataframe with at least the column "created_dt"
    Return:
        object: dataframe with the columns - acquisition_date, acquisitions
    """
    simplified = transformed_cons[["created_dt"]]
    grouped = simplified.groupby(pd.to_datetime(simplified['created_dt']).dt.date).count()
    grouped.rename(columns={'created_dt': 'acquisitions'}, inplace=True)
    grouped.reset_index(inplace=True)
    grouped.rename(columns={'created_dt': 'acquisition_date'}, inplace=True)
    return grouped


def load_cons_data(people: pd.DataFrame, aggregate: pd.DataFrame, people_file_name="people.csv",
                   aggregate_file_name="aggregate.csv") -> None:
    """
    This function simple saves pandas data frames to the working directory.
    However, in the future this could be extended to upload these files to a SQL database

    Args:
        people (object): This is a dataframe with the columns - email, code, is_unsub, created_dt, updated_dt
        aggregate (object): This is a dataframe with the columns - acquisition_date, acquisitions
        people_file_name (str): Defaults to "people.csv" saved in the cwd
        aggregate_file_name (str): Defaults to "aggregate.csv" saved in the cwd
    Return:
         None
    """
    people.to_csv(people_file_name, index=False)
    aggregate.to_csv(aggregate_file_name, index=False)
    return


if __name__ == "__main__":
    input_dataframes = extract_cons_data(CONS_URL, CONS_EMAIL_URL, CONS_SUB_URL)
    transformed_dataframe = transform_cons_data(*input_dataframes)
    aggregated_dataframe = aggregate_cons_data(transformed_dataframe)
    load_cons_data(transformed_dataframe, aggregated_dataframe)

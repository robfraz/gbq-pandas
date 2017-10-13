# Standard library imports
from tempfile import NamedTemporaryFile
from uuid import uuid4
from itertools import islice

# Third party imports
import pandas as pd


def df_to_table(df,
                table,
                write_disposition='WRITE_EMPTY',
                blocking=True):
    """Upload a Pandas DataFrame to Google BigQuery

    Args:
        df (DataFrame): The Pandas DataFrame to be uploaded.
        table (google.cloud.bigquery.Table): BigQuery table object.
        write_disposition (str): Either 'WRITE_EMPTY', 'WRITE_TRUNCATE', or
            'WRITE_APPEND'; the default is 'WRITE_EMPTY'.
        blocking (bool): Set to False if you don't want to block until the job
           is complete.

    Returns:
        google.cloud.bigquery.Job: The file upload job object.  If you have set
            blocking=False, this can be used to check for job completion.
    """
    with NamedTemporaryFile(mode='w',
                            encoding='UTF-8',
                            prefix="df_to_table_",
                            suffix=".csv") as writebuf:
        df.to_csv(writebuf, index=False, encoding='UTF-8')
        writebuf.flush()

        # Annoyingly, df.to_csv above requires a non binary mode file handle,
        # whereas table.upload_from_file below requires a binary mode file
        # handle, so we end up with nested context handlers.
        with open(writebuf.name, mode='rb') as readbuf:
            job = table.upload_from_file(readbuf,
                                         encoding='UTF-8',
                                         source_format='CSV',
                                         skip_leading_rows=1,
                                         create_disposition='CREATE_IF_NEEDED',
                                         write_disposition=write_disposition)

    if blocking:
        job.result()

    return job


def query_to_df(sql, client):
    """Run a Google BigQuery query, and return the result in a Pandas Dataframe

    The query must be a single SQL statement

    Args:
        sql (str): A string containing a single SQL statement.
        client (google.cloud.bigquery.Client): BigQuery client object.

    Returns
        DataFrame: A Pandas DataFrame containing the result of the query.
    """
    job = client.run_async_query(str(uuid4()), sql)
    job.use_legacy_sql = False
    result = job.result()
    return table_to_df(result.destination)


def table_to_df(table, limit=None):
    """Download a table from Google BigQuery into a dataframe, with optional row limit

    Args:
        table (google.cloud.bigquery.Table): BigQuery table object.
        limit (None|int): The default is limit=None (i.e. all rows in table); set to
            zero to get an empty DataFrame with the column names set, or a positive
            number to limit the maximum number of rows fetched into the DataFrame.

    Returns:
        DataFrame: A Pandas DataFrame containing the table data.
    """
    if limit and limit < 0:
        limit = None

    table.reload()
    return pd.DataFrame(data=list(islice(table.fetch_data(), 0, limit)),
                        columns=[column.name for column in table.schema])

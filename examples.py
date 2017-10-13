# Standard library imports
from pathlib import Path

# Third-party imports
import numpy as np
import pandas as pd
from google.cloud.bigquery import Client, SchemaField

# Local imports
import gbq_pandas as gbq


def main():
    # Preamble - gets some creds and a valid Google BigQuery Client object.
    gbq_creds_path = Path("~/.gbq-key.json").expanduser()
    client = Client.from_service_account_json(gbq_creds_path)

    # Example of df_to_table().
    schema = [
        SchemaField('field1', 'float64', 'REQUIRED'),
        SchemaField('field2', 'float64', 'REQUIRED'),
        SchemaField('field3', 'float64', 'REQUIRED'),
        SchemaField('field4', 'float64', 'REQUIRED')
    ]
    table = client.dataset('my_dataset').table('example_table_1', schema)
    df1 = pd.DataFrame(np.random.rand(10, 4))
    gbq.df_to_table(df1, table)

    # Example table_to_df().
    dataset = client.dataset('my_dataset')
    table = dataset.table('example_table_1')
    df2 = gbq.table_to_df(table)
    df2.info()

    # Example of query_to_df().
    query = 'select field1, field2 from my_dataset.example_table_1;'
    df3 = gbq.query_to_df(query, client)
    df3.info()


if __name__ == '__main__':
    main()

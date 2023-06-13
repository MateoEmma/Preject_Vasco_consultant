
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import io

def load_data_to_bigquery(event, context):

    # The authentication json file is downloaded
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('user_credential')
    blob = bucket.blob('vasco-data-engineer-mateo-9d05b06c73ff.json')
    blob.download_to_filename('/tmp/vasco-data-engineer-mateo-9d05b06c73ff.json')
    
    # Especificar la ruta al archivo de credenciales y el ID del proyecto
    key_file = '/tmp/vasco-data-engineer-mateo-9d05b06c73ff.json'
    project_id = 'vasco-data-engineer-mateo'

    # Create credentials from the credentials file
    credentials = service_account.Credentials.from_service_account_file(key_file)

    # Create a Cloud Storage client with the specified credentials
    storage_client = storage.Client(credentials=credentials, project=project_id)

   
    # the bucket and file name
    bucket_name = event['bucket']
    file_name = event['name']

    if file_name == 'surveys':
        # Download the file from Cloud Storage to an in-memory variable
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        data = blob.download_as_string()

        # Create a pandas DataFrame with the data
        df = pd.read_csv(io.BytesIO(data))

        # BigQuery credentials
        client = bigquery.Client(credentials=credentials, project=project_id)

        # the dataset and the table into which the data will be loaded
        dataset_id = 'Surveys_stage'
        table_id = 'surveys_stg'

        # Create a reference to the table
        table_ref = client.dataset(dataset_id).table(table_id)

        # Load the DataFrame without transformations into the BigQuery surveys_stg table
        job_config = bigquery.LoadJobConfig(
            write_disposition='WRITE_TRUNCATE'
        )

        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()

        # Applies the necessary transformations to the DataFrame
        df = df.drop_duplicates()
        df['Student'] = df['Student'].fillna('No')
        df['Employment'] = df['Employment'].fillna('Full-time')
        df = df.dropna(thresh=len(df.columns)-3)

        # the dataset and the table into which the data will be loaded
        dataset_id = 'Surveys_productivo'
        table_id = 'surveys_prd'

        # Create a reference to the table
        table_ref = client.dataset(dataset_id).table(table_id)

        # Load the DataFrame into the BigQuery table with UTF 8 decoding
        job_config = bigquery.LoadJobConfig(
            encoding='UTF-8',
            write_disposition='WRITE_TRUNCATE'
        )
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()





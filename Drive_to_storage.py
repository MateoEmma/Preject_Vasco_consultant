# We define the load_file function to instantiate it in Cloud Functions
def load_file():

    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    from google.cloud import storage
    import io
    import zipfile

    # authentication
    key_file = 'vasco-data-engineer-mateo-9d05b06c73ff.json'
    project_id = 'vasco-data-engineer-mateo'
    bucket_name = 'encuestas-bucket'
    file_id = '1Nm1B8NE66ly1XzdU0mAYIuwhtwNG4pk-'
    destination_blob_name = 'surveys'

    # Create an instance of the Google Drive service client
    creds = service_account.Credentials.from_service_account_file(key_file)
    drive_service = build('drive', 'v3', credentials=creds)

    # Download the file from Google Drive
    request = drive_service.files().get_media(fileId=file_id)
    zip_file = io.BytesIO()
    downloader = MediaIoBaseDownload(zip_file, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print(f'Download {int(status.progress() * 100)}.')
        zip_file.seek(0)

    # Unzip the ZIP file
    with zipfile.ZipFile(zip_file, 'r') as myzip:
        for file in myzip.namelist():
            if file.endswith('.csv'):
                with myzip.open(file) as myfile:
                    file_contents = myfile.read()

    # Create an instance of the Cloud Storage service client
    storage_client = storage.Client(project=project_id, credentials=creds)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Upload the file to Cloud Storage
    file = io.BytesIO(file_contents)
    blob.upload_from_file(file)
    print(f'File {file_id} uploaded to {destination_blob_name}.')


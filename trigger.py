from google.oauth2 import service_account
from googleapiclient.discovery import build

# Autentificacion
key_file = 'vasco-data-engineer-mateo-9d05b06c73ff.json'
file_id = '1Nm1B8NE66ly1XzdU0mAYIuwhtwNG4pk-'
channel_id = 'my-channel'
channel_token = 'my-token'
address = 'https://europe-west1-vasco-data-engineer-mateo.cloudfunctions.net/funcion-load-file'

# Crea una instancia del cliente de servicio de Google Drive
creds = service_account.Credentials.from_service_account_file(key_file)
drive_service = build('drive', 'v3', credentials=creds)

# Crea un canal de notificación para el archivo en Google Drive
body = {
    'id': channel_id,
    'type': 'web_hook',
    'address': address,
    'token': channel_token
}
response = drive_service.files().watch(fileId=file_id, body=body).execute()
print(response)

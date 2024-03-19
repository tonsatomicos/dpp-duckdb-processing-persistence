import os
import duckdb
import datetime
import shutil

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from dotenv import load_dotenv

def download_gdrive_files_from_folder(credentials, raw_folder_items, processed_folder_items, folder_id):
    try:
        SCOPES = ['https://www.googleapis.com/auth/drive']
        creds = None

        if os.path.exists(os.path.join(credentials,'token.json')):
            creds = Credentials.from_authorized_user_file(os.path.join(credentials,'token.json'))

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())

            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    os.path.join(credentials,'credentials.json'), SCOPES)
                creds = flow.run_local_server(port=0)

            with open(os.path.join(credentials,'token.json'), 'w') as token:
                token.write(creds.to_json())

        service = build('drive', 'v3', credentials=creds)

        destination_folder = raw_folder_items
        processed_folder = processed_folder_items

        results = service.files().list(
            q=f"'{folder_id}' in parents",
            fields="files(id, name)"
        ).execute()

        items = results.get('files', [])

        if not items:
            print('No files found.')

        else:
            for item in items:
                file_id = item['id']
                file_name = item['name']
                destination_path = os.path.join(destination_folder, file_name)
                processed_path = os.path.join(processed_folder, file_name)
                
                if os.path.exists(destination_path) or os.path.exists(processed_path):
                    pass
                else:
                    request = service.files().get_media(fileId=file_id)
                    with open(destination_path, 'wb') as f:
                        f.write(request.execute())

    except HttpError as e:
        print("An HTTP error occurred during the download:", e)

    except OSError as e:
        print("A system error occurred during the download:", e)
        
    except Exception as e:
        print("An error occurred during the download:", e)
    
def processing_persistence(raw_folder_items, processed_folter_items, date_now):
    try:
        conn = duckdb.connect(database=':memory:')
        
        conn.execute("INSTALL postgres")
        conn.execute("LOAD postgres")
        conn.execute("ATTACH 'dbname=unifor_duckdb user=unifor password=unifor host=localhost port=5437' AS db (TYPE postgres)")
        conn.execute("CREATE TEMPORARY TABLE temp_table (COD_UF VARCHAR, COD_MUN VARCHAR, COD_ESPECIE VARCHAR, LATITUDE VARCHAR, LONGITUDE VARCHAR, NV_GEO_COORD VARCHAR, NOME_ARQUIVO VARCHAR, DT_CADASTRO DATETIME)")

        for filename in os.listdir(raw_folder_items):
            if filename.endswith(".csv"):
                conn.execute(f"INSERT INTO temp_table SELECT A.*, '{filename}' NOME_ARQUIVO, '{date_now}' DT_CADASTRO FROM read_csv('{os.path.join(raw_folder_items,filename)}', delim = ';', header = true) A")
                shutil.move(os.path.join(raw_folder_items,filename), os.path.join(processed_folter_items,filename))

        conn.execute("INSERT INTO db.duckdb_ibge SELECT * FROM temp_table")

    except Exception as e:
        print("An error occurred during processing:", e)

def main():

    date_now = datetime.datetime.now()
    raw_folder_items = "C://Tecnology//Projects//duckdb-processing-persistence//data//raw"
    processed_folder_items = "C:/Tecnology//Projects//duckdb-processing-persistence//data//processed"
    credentials = 'C://Tecnology//Projects//duckdb-processing-persistence//config//credentials'
    
    dotenv_path = 'C://Tecnology//Projects//duckdb-processing-persistence//config//.env'
    load_dotenv(dotenv_path)

    download_gdrive_files_from_folder(credentials, raw_folder_items, processed_folder_items, folder_id=os.getenv("FOLDER_ID"))
    if os.listdir(raw_folder_items):
        processing_persistence(raw_folder_items, processed_folder_items, date_now)
    else:
        pass

if __name__ == '__main__':
    main()



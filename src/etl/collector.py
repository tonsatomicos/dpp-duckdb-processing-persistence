import os
import duckdb
import datetime
import shutil

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from dotenv import load_dotenv


class GDriveFolder:
    def __init__(self, credentials, raw_folder_items, processed_folder_items, g_drive_folder_id):
        self.credentials = credentials
        self.raw_folder_items = raw_folder_items
        self.processed_folder_items = processed_folder_items
        self.g_drive_folder_id = g_drive_folder_id
        self.SCOPES = ['https://www.googleapis.com/auth/drive']
        self.service = None
        self.new_files = None

    def authenticate_and_build_service(self):
        try:
            creds = None
            scopes = self.SCOPES
            token_path = os.path.join(self.credentials,'token.json')
            credentials_path = os.path.join(self.credentials,'credentials.json')

            if os.path.exists(token_path):
                creds = Credentials.from_authorized_user_file(token_path)

            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        credentials_path, scopes)
                    creds = flow.run_local_server(port=0)

                with open(token_path, 'w') as token:
                    token.write(creds.to_json())

            self.service = build('drive', 'v3', credentials=creds)
        
        except Exception as e:
            print('Error authenticating: ', e)
    
    def download_files(self):
        if not self.service:
            print('Error authenticating.')
            return
        
        try:
            service = self.service
            g_drive_folder_id = self.g_drive_folder_id

            results = service.files().list(
                q=f"'{g_drive_folder_id}' in parents",
                fields="files(id, name)"
            ).execute()

            items = results.get('files', [])

            if not items:
                print('No files found.')

            else:
                for item in items:
                    file_id = item['id']
                    file_name = item['name']
                    raw_folder_items = os.path.join(self.raw_folder_items, file_name)
                    processed_folder_items = os.path.join(self.processed_folder_items, file_name)
                    
                    if os.path.exists(raw_folder_items) or os.path.exists(processed_folder_items):
                        pass
                    else:
                        request = service.files().get_media(fileId=file_id)
                        with open(raw_folder_items, 'wb') as f:
                            f.write(request.execute())
                        self.new_files = True

        except OSError as e:
            print("A system error occurred during the download:", e)
            
        except Exception as e:
            print("An error occurred during the download:", e)

class DataProcessor:
    def __init__(self, raw_folder_items, processed_folder_items, db_plugin, db_name, db_user, db_pass, db_host, db_port, db_query):
        self.raw_folder_items = raw_folder_items
        self.processed_folder_items = processed_folder_items
        self.db_plugin = db_plugin
        self.db_name = db_name
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_host = db_host
        self.db_port = db_port
        self.db_query = db_query
        self.date_now = datetime.datetime.now()
        self.conn_duckdb = None
        self.conn_duckdb_postgres = None

    def authenticate_duckdb(self):
        try:
            self.conn_duckdb = duckdb.connect(database=':memory:')
            self.conn_duckdb.execute(f"INSTALL {self.db_plugin}")
            self.conn_duckdb.execute(f"LOAD {self.db_plugin}")
        
        except Exception as e:
                    print("An error occurred while connecting to the database: ", e)

    def authenticate_duckdb_postgres(self):
        try:   
            self.conn_duckdb.execute(f"ATTACH 'dbname={self.db_name} user={self.db_user} password={self.db_pass} host={self.db_host} port={self.db_port}' AS db (TYPE {self.db_plugin})")
            self.conn_duckdb_postgres = True

        except Exception as e:
                    print("An error occurred while connecting to the database: ", e)
                    
    def process_and_persist(self):
        if not self.conn_duckdb:
            print("An error occurred while connecting to the database.")
            return
        
        try:
            self.conn_duckdb.execute(self.db_query)

            raw_folder_items = self.raw_folder_items
            processed_folder_items = self.processed_folder_items
            date_now = self.date_now

            for filename in os.listdir(raw_folder_items):
                if filename.endswith(".csv"):
                    self.conn_duckdb.execute(f"INSERT INTO temp_table SELECT A.*, '{filename}' NOME_ARQUIVO, '{date_now}' DT_CADASTRO FROM read_csv('{os.path.join(raw_folder_items,filename)}', delim = ';', header = true) A")
                    shutil.move(os.path.join(raw_folder_items,filename), os.path.join(processed_folder_items,filename))

            self.conn_duckdb.execute("INSERT INTO db.duckdb_ibge SELECT * FROM temp_table")

        except Exception as e:
            print("An error occurred during processing: ", e)

        finally:
            if 'conn' in locals():
                    self.conn_duckdb.close()     

def main():
    # GDriveFolder class configs
    credentials = 'C://Tecnology//Projects//unifor-challenge-data-processing//config//credentials//'
    raw_folder_items = "C://Tecnology//Projects//unifor-challenge-data-processing//data//raw//"
    processed_folder_items = "C:/Tecnology//Projects//unifor-challenge-data-processing//data//processed//"
    dotenv_path = 'C://Tecnology//Projects//unifor-challenge-data-processing//config//.env'
    load_dotenv(dotenv_path)
    g_drive_folder_id=os.getenv("g_drive_folder_id")

    # DataProcessor class configs
    db_plugin = 'postgres'
    db_name = 'unifor_duckdb'
    db_user = 'unifor'
    db_pass = 'unifor'
    db_host = 'localhost'
    db_port = '5437'
    db_query = 'CREATE TEMPORARY TABLE temp_table (COD_UF VARCHAR, COD_MUN VARCHAR, COD_ESPECIE VARCHAR, LATITUDE VARCHAR, LONGITUDE VARCHAR, NV_GEO_COORD VARCHAR, NOME_ARQUIVO VARCHAR, DT_CADASTRO DATETIME)'
    
    # GdriveFolder class
    obj_gdrivefolder = GDriveFolder(credentials, raw_folder_items, processed_folder_items, g_drive_folder_id)
    obj_gdrivefolder.authenticate_and_build_service()

    # DataProcessor class
    obj_dataprocessor = DataProcessor(raw_folder_items, processed_folder_items, db_plugin, db_name, db_user, db_pass, db_host, db_port, db_query)
    obj_dataprocessor.authenticate_duckdb()
    obj_dataprocessor.authenticate_duckdb_postgres()

    # 
    if obj_gdrivefolder.service and obj_dataprocessor.conn_duckdb and obj_dataprocessor.conn_duckdb_postgres:
            obj_gdrivefolder.download_files()

            if obj_gdrivefolder.new_files:
                obj_dataprocessor.process_and_persist()

if __name__ == '__main__':
    main()



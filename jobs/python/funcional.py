# %% [markdown]
# ### Importing Libraries

# %%
import os
import duckdb
import datetime
import shutil

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from dotenv import load_dotenv

# %% [markdown]
# ### Getting files from Google Drive

# %%
def download_files_from_folder(FOLDER_ID):
    SCOPES = ['https://www.googleapis.com/auth/drive']
    creds = None

    if os.path.exists('C://Tecnology//Projects//gdrive-duckdb-postgres//credentials//token.json'):
        creds = Credentials.from_authorized_user_file('C://Tecnology//Projects//gdrive-duckdb-postgres//credentials//token.json')

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())

        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'C://Tecnology//Projects//gdrive-duckdb-postgres//credentials//credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

        with open('C://Tecnology//Projects//gdrive-duckdb-postgres//credentials//token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('drive', 'v3', credentials=creds)

    destination_folder = 'C://Tecnology//Projects//gdrive-duckdb-postgres//raw'
    processed_folder = 'C://Tecnology//Projects//gdrive-duckdb-postgres//processed'  

    results = service.files().list(
        q=f"'{FOLDER_ID}' in parents",
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
                print(f"The file '{file_name}' already exists in the destination folder.")
            else:
                request = service.files().get_media(fileId=file_id)
                with open(destination_path, 'wb') as f:
                    f.write(request.execute())
                    # print(f"Downloaded {file_name}")

# %% [markdown]
# ### Reading CSV with Duckdb and load into postgres

# %%
conn = duckdb.connect(database=':memory:')
conn.execute("CREATE TEMPORARY TABLE temp_table (COD_UF VARCHAR, COD_MUN VARCHAR, COD_ESPECIE VARCHAR, LATITUDE VARCHAR, LONGITUDE VARCHAR, NV_GEO_COORD VARCHAR, NOME_ARQUIVO VARCHAR, DT_CADASTRO DATETIME)")

# %%
raw_folder = "C://Tecnology//Projects//gdrive-duckdb-postgres//raw"
processed_folder = "C://Tecnology//Projects//gdrive-duckdb-postgres//processed"

# Iterar sobre os arquivos na pasta "raw"
for filename in os.listdir(raw_folder):
    date_now = datetime.datetime.now()

    if filename.endswith(".csv"):
        conn.execute(f"INSERT INTO temp_table SELECT A.*, '{filename}' NOME_ARQUIVO, '{date_now}' DT_CADASTRO FROM read_csv('raw/{filename}', delim = ';', header = true) A")
        shutil.move(f"{raw_folder}/{filename}",f"{processed_folder}/{filename}")

# %%
conn.execute("INSTALL postgres")
conn.execute("LOAD postgres")
conn.execute("ATTACH 'dbname=testes user=lucas password=lucas host=localhost port=5437' AS db (TYPE postgres)")
conn.execute("INSERT INTO db.enderecos SELECT * FROM temp_table")


# %%
def main():
    load_dotenv()
    download_files_from_folder(FOLDER_ID=os.getenv("FOLDER_ID"))

if __name__ == '__main__':
    main()



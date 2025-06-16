from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
import os
from dotenv import load_dotenv

def upload_file_to_adlsg2(dir_name:str, raw_file_name:str):

       # Load .env variables
       load_dotenv()

       # Credentials to access ADLSg2
       
       tenant_id = os.getenv("AZURE_TENANT_ID")
       client_id = os.getenv("AZURE_CLIENT_ID")
       client_secret = os.getenv("AZURE_CLIENT_SECRET")

       credential = ClientSecretCredential(
              tenant_id=tenant_id,
              client_id=client_id,
              client_secret=client_secret
       )

       storage_acc_url = "https://fantasybookdatastorage.dfs.core.windows.net"

       container_name = "bronze"
       directory_name = dir_name
       file_name = raw_file_name

       # Accessing ADLSg2

       service_client = DataLakeServiceClient(account_url=storage_acc_url, credential=credential)
       container_client = service_client.get_file_system_client(container_name)
       directory_client = container_client.get_directory_client(directory_name)
       directory_client.create_directory()
       file_client = directory_client.create_file(file_name)

       # Reading Data from Docker Container
       docker_container_file_path = f"/opt/airflow/dags/bronze/bronze/{raw_file_name}"
       with open(docker_container_file_path, "rb") as f:
              data = f.read()

       # Writing data from the Docker Container to an ADLSg2 container
       file_client.append_data(data=data, offset=0, length=len(data))
       file_client.flush_data(len(data))

def upload_all_files_to_adlsg2():
       upload_file_to_adlsg2("library", "fantasy_book_library.parquet")
       upload_file_to_adlsg2("authors", "fantasy_authors.parquet")
       upload_file_to_adlsg2("ratings", "fantasy_book_ratings.parquet")
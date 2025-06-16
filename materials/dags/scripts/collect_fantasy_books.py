import requests
import pandas as pd
import json
import os

fantasy_books_url = "https://openlibrary.org/subjects/fantasy.json"

def save_api_page(filename:str, page_number:int, data):
       output_path = "materials/dags/raw/fantasy_books_library/"
       os.makedirs(output_path, exist_ok=True)

       file_path = os.path.join(output_path, f"{filename}_page_{page_number}.json")
       with open(file_path, "w") as f:
              json.dump(data, f, indent=4)

def collect_all_fantasy_books(api_url:str):
       
       offset = 0

       while True:
              try:
                     response = requests.get(api_url, params={"offset": offset, "limit":1000})
                     data_in_page = response.json()
                     data_in_page_length = len(data_in_page["works"])
                     print(data_in_page_length)
                     save_api_page("fantasy_books", page_number=int(offset/1000), data=data_in_page)
              except requests.exceptions.JSONDecodeError:
                     print(offset/1000)
                     print("Status code: ", response.status_code)
                     print("Response text: ", response.text)
              if not data_in_page:
                     print("No data remaining")
                     break
              if data_in_page_length < 1000:
                     break

              offset += 1000

def create_book_library_file(folder_path:str):

       book_library_dfs = []

       for file_name in os.listdir(folder_path):
              if file_name.endswith(".json"):
                     with open(os.path.join(folder_path, file_name)) as f:
                            books_data = json.load(f)
                            df_book_libary = pd.json_normalize(books_data["works"])
                            book_library_dfs.append(df_book_libary)
                            
       df_combined_book_libary = pd.concat(book_library_dfs, ignore_index=True)
       bronze_path = "materials/dags/bronze/"
       os.makedirs(bronze_path, exist_ok=True)

       df_combined_book_libary.to_parquet(f"{bronze_path}fantasy_book_library.parquet", index=False)

def collect_fantasy_books_bronze():
       collect_all_fantasy_books(fantasy_books_url)
       create_book_library_file("materials/dags/raw/fantasy_books_library")


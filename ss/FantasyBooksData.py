import requests
import pandas as pd
import json
import os
from pathlib import Path

url = "https://openlibrary.org/subjects/fantasy.json"

with open("bronze/authors/authors_id_list.txt", "r") as f:
       content = [line.strip() for line in f.readlines()]

author_id_list = content

def collect_authors_data(authorIDs:list):
       
       for authorId in authorIDs:
              try:
                     authors_url = f"https://openlibrary.org/authors/{authorId}.json"
                     authors_response = requests.get(authors_url)
                     authors_data = authors_response.json()
                     with open(f"bronze/authors/{authorId}.json", "w") as f:
                            json.dump(authors_data, f, indent=4)
              except requests.exceptions.JSONDecodeError:
                     print("Status code: ", authors_response.status_code)
                     print("Response text: ", authors_response.text)

def save_api_page(filename:str, page_number:int, data):
       with open(f"bronze/fantasy_books_library/{filename}_page_{page_number}.json", "w") as f:
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

def combine_library_files(folder_path:str):
       folder = folder_path
       combined_files = []

       for filename in os.listdir(folder):
              if filename.endswith(".json"):
                     with open(os.path.join(folder, filename), "r") as f:
                            content = json.load(f)
                            records = content.get("works", [])
                            combined_files.extend(records)
       return combined_files

def create_libary_table(folder_path:str):

       all_fantasy_books = combine_library_files(folder_path)

       books_df = pd.DataFrame(all_fantasy_books)
       books_df = books_df.drop(columns=[
                                   'ia_collection', 'printdisabled', 'lending_edition',
                                   'lending_identifier', 'ia',
                                   'public_scan', 'has_fulltext', 'availability'
                                   ])

       return books_df

def get_book_keys(raw_keys:list) -> list:
       book_keys = []

       for key in raw_keys:
              actual_key = key.split("/")[-1]
              book_keys.append(actual_key)

       return book_keys

def collect_book_reviews(book_keys:list):

       for key in book_keys:
              ratings_url = f"https://openlibrary.org/works/{key}/ratings.json"
              rating_response = requests.get(ratings_url)
              book_rating = rating_response.json()
              with open(f"bronze/books_ratings/{key}_rating.json", "w") as f:
                     json.dump(book_rating, f, indent=4)

def collect_book_works(book_keys:list):

       for key in book_keys:
              try:
                     works_url = f"https://openlibrary.org/works/{key}.json"
                     works_response = requests.get(works_url)
                     book_works = works_response.json()
                     with open(f"bronze/books_works/{key}_works.json", "w") as f:
                            json.dump(book_works, f, indent=4)
              except requests.exceptions.JSONDecodeError:
                     print("Status code: ", works_response.status_code)
                     print("Response text: ", works_response.text)

def combine_book_works_files(folder_path:str, output_file_name:str):
       input_folder = Path(folder_path)
       output_file = Path(output_file_name)

       all_data = []

       for file in input_folder.glob("*.json"):
              with open(file, "r") as f:
                     data = json.load(f)
                     all_data.append(data)

       with open(f"bronze/books_works/{output_file}", "w") as f:
              json.dump(all_data, f, indent=4)

def create_book_ratings_file(folder_path:str):

       book_ratings_dfs = []
       book_keys = []

       for file_name in os.listdir(folder_path):
              if file_name.endswith(".json"):
                     with open(os.path.join(folder_path, file_name)) as f:
                            ratings_data = json.load(f)
                            df_book_ratings = pd.json_normalize(ratings_data)
                            book_ratings_dfs.append(df_book_ratings)
                            book_keys.append(file_name)

       df_combined_book_ratings = pd.concat(book_ratings_dfs, ignore_index=True)
       df_combined_book_ratings["BookKey"] = book_keys

       df_combined_book_ratings.to_parquet("fantasy_book_ratings.parquet", index=False)

def create_book_library_file(folder_path:str):

       book_library_dfs = []

       for file_name in os.listdir(folder_path):
              if file_name.endswith(".json"):
                     with open(os.path.join(folder_path, file_name)) as f:
                            books_data = json.load(f)
                            df_book_libary = pd.json_normalize(books_data["works"])
                            book_library_dfs.append(df_book_libary)
                            
       df_combined_book_libary = pd.concat(book_library_dfs, ignore_index=True)
       df_combined_book_libary.to_parquet("fantasy_book_library.parquet", index=False)

def create_authors_file(folder_path:str):
       author_dfs = []

       for file_name in os.listdir(folder_path):
              if file_name.endswith(".json"):
                     with open(os.path.join(folder_path, file_name)) as f:
                            author_data = json.load(f)
                            df_author = pd.json_normalize(author_data)
                            author_dfs.append(df_author)

       df_authors = pd.concat(author_dfs, ignore_index=True)
       df_authors.to_parquet("fantasy_authors.parquet", index=False)


# library = create_libary_table("bronze/fantasy_books_library/")
# raw_book_keys = library["key"].tolist()
# actual_book_keys = get_book_keys(raw_book_keys)

# collect_authors_data(author_id_list)
# create_authors_file("bronze/authors/")

create_authors_file("bronze/authors/")






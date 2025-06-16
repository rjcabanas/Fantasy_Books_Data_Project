import requests
import pandas as pd
import json
import os

def get_authors_key_list():
    df_books = pd.read_parquet("materials/dags/bronze/fantasy_book_library.parquet")

    authors_list = df_books["authors"].tolist()
    author_key_list = []

    for author in authors_list:
        try:
            author_key = author[0]["key"]
            author_key = author_key.split("/")[2]
            author_key_list.append(author_key)
        except(IndexError):
            continue

    author_key_set = set(author_key_list)    
    
    return author_key_set

def collect_authors_data(authorIDs:set):
    
    output_path = "materials/dags/raw/authors/"
    os.makedirs(output_path, exist_ok=True)

    for authorId in authorIDs:
            try:
                    authors_url = f"https://openlibrary.org/authors/{authorId}.json"
                    authors_response = requests.get(authors_url)
                    authors_data = authors_response.json()
                    file_path = os.path.join(output_path, f"{authorId}.json")
                    with open(file_path, "w") as f:
                        json.dump(authors_data, f, indent=4)
            except requests.exceptions.JSONDecodeError:
                    print("Status code: ", authors_response.status_code)
                    print("Response text: ", authors_response.text)

def create_authors_file(folder_path:str):
    author_dfs = []

    for file_name in os.listdir(folder_path):
            if file_name.endswith(".json"):
                    with open(os.path.join(folder_path, file_name)) as f:
                        author_data = json.load(f)
                        df_author = pd.json_normalize(author_data)
                        author_dfs.append(df_author)

    df_authors = pd.concat(author_dfs, ignore_index=True)
    bronze_path = "materials/dags/bronze/"
    os.makedirs(bronze_path, exist_ok=True)
    df_authors.to_parquet(f"{bronze_path}fantasy_authors.parquet", index=False)

def collect_authors_bronze():
    unique_author_keys = get_authors_key_list()
    collect_authors_data(unique_author_keys)
    create_authors_file("materials/dags/raw/authors")


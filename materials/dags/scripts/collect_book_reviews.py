import requests
import pandas as pd
import json
import os

def get_book_keys() -> list:
    df_books = pd.read_parquet("materials/dags/bronze/fantasy_book_library.parquet")

    raw_book_keys = df_books["key"].tolist()
    cleaned_book_keys = []

    for key in raw_book_keys:
        actual_key = key.split("/")[-1]
        cleaned_book_keys.append(actual_key)

    return cleaned_book_keys

def collect_book_reviews(book_keys:list):

    output_path = "materials/dags/raw/book_ratings/"
    os.makedirs(output_path, exist_ok=True)
    
    for key in book_keys:
            try:
                ratings_url = f"https://openlibrary.org/works/{key}/ratings.json"
                rating_response = requests.get(ratings_url)
                book_rating = rating_response.json()
                file_path = os.path.join(output_path, f"{key}_rating.json")
                with open(file_path, "w") as f:
                        json.dump(book_rating, f, indent=4)
            except ConnectionError:
                 continue
            
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

    bronze_path = "materials/dags/bronze/"
    os.makedirs(bronze_path, exist_ok=True)
    df_combined_book_ratings.to_parquet(f"{bronze_path}fantasy_book_ratings.parquet", index=False)

def collect_book_ratings_bronze():
    book_keys = get_book_keys()
    collect_book_reviews(book_keys)

    output_path = "materials/dags/raw/book_ratings/"
    os.makedirs(output_path, exist_ok=True)

    create_book_ratings_file(output_path)


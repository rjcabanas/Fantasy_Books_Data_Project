from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from scripts.collect_fantasy_books import collect_fantasy_books_bronze
from scripts.collect_authors import collect_authors_bronze
from scripts.collect_book_reviews import collect_book_ratings_bronze
from scripts.push_to_adlsg2 import *

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 26),
    'retries': 0 
}

with DAG(
    "Ingestion_DAG", 
    default_args=default_args, 
    catchup=False, 
    schedule='@once'
) as dag:

    collect_books_task = PythonOperator(
        task_id="collect_books",
        python_callable=collect_fantasy_books_bronze
    )

    collect_authors_task = PythonOperator(
        task_id="collect_authors",
        python_callable=collect_authors_bronze
    )

    collect_ratings_task = PythonOperator(
        task_id="collect_ratings",
        python_callable=collect_book_ratings_bronze
    )

    upload_to_adlsg2_task = PythonOperator(
        task_id="upload_to_adlsg2",
        python_callable=upload_all_files_to_adlsg2
    )

    collect_books_task >> collect_authors_task
    collect_books_task >> collect_ratings_task
    collect_authors_task >> upload_to_adlsg2_task
    collect_ratings_task >> upload_to_adlsg2_task
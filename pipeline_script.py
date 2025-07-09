import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr, lit, year, coalesce, floor
from pyspark.sql.types import DoubleType, IntegerType, StringType, BooleanType, DateType


BASE_DIR = 'data_lake'
LANDING_ZONE = os.path.join(BASE_DIR, 'landing')
RAW_ZONE = os.path.join(BASE_DIR, 'raw')
CURATED_ZONE = os.path.join(BASE_DIR, 'curated')

LANDING_IMDB_DIR = os.path.join(LANDING_ZONE, 'imdb')
RAW_IMDB_DIR = os.path.join(RAW_ZONE, 'imdb')

IMDB_TITLE_BASICS_FILE_LANDING = os.path.join(LANDING_IMDB_DIR, 'title.basics.tsv') 
IMDB_TITLE_RATINGS_FILE_LANDING = os.path.join(LANDING_IMDB_DIR, 'title.ratings.tsv') 
IMDB_NAME_BASICS_FILE_LANDING = os.path.join(LANDING_IMDB_DIR, 'name.basics.tsv') 
IMDB_TITLE_AKAS_FILE_LANDING = os.path.join(LANDING_IMDB_DIR, 'title.akas.tsv') 
IMDB_TITLE_CREW_FILE_LANDING = os.path.join(LANDING_IMDB_DIR, 'title.crew.tsv')
IMDB_TITLE_PRINCIPALS_FILE_LANDING = os.path.join(LANDING_IMDB_DIR, 'title.principals.tsv') 

IMDB_BASICS_FILE_RAW = os.path.join(RAW_IMDB_DIR, 'imdb_title_basics.parquet')
IMDB_RATINGS_FILE_RAW = os.path.join(RAW_IMDB_DIR, 'imdb_ratings.parquet')
IMDB_NAME_BASICS_FILE_CURATED = os.path.join(RAW_IMDB_DIR, 'imdb_name_basics.parquet')

for zone_path in [LANDING_ZONE, RAW_ZONE, CURATED_ZONE]:
    os.makedirs(zone_path, exist_ok=True)
    print(f"Directorio creado/verificado: {zone_path}")


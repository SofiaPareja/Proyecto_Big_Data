# src/config.py

import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data_lake")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")


LANDING_ZONE_PATH = os.path.join(DATA_DIR, "landing", "imdb")
RAW_ZONE_PATH = os.path.join(DATA_DIR, "raw", "imdb")
CURATED_ZONE_PATH = os.path.join(DATA_DIR, "curated", "imdb")


REGION_OF_INTEREST = "UY" 


#se usa StringType para columnas que pueden tener '\N' y despeus se castean a los tipos necesarios
SCHEMAS = {
    "title.basics": StructType([
        StructField("tconst", StringType(), True),
        StructField("titleType", StringType(), True),
        StructField("primaryTitle", StringType(), True),
        StructField("originalTitle", StringType(), True),
        StructField("isAdult", StringType(), True),
        StructField("startYear", StringType(), True), 
        StructField("endYear", StringType(), True),   
        StructField("runtimeMinutes", StringType(), True), 
        StructField("genres", StringType(), True)
    ]),
    "title.akas": StructType([
        StructField("titleId", StringType(), True),
        StructField("ordering", StringType(), True), 
        StructField("title", StringType(), True),
        StructField("region", StringType(), True),
        StructField("language", StringType(), True),
        StructField("types", StringType(), True),
        StructField("attributes", StringType(), True),
        StructField("isOriginalTitle", StringType(), True) 
    ]),
    "title.ratings": StructType([
        StructField("tconst", StringType(), True),
        StructField("averageRating", StringType(), True), 
        StructField("numVotes", StringType(), True)      
    ]),
    "title.crew": StructType([
        StructField("tconst", StringType(), True),
        StructField("directors", StringType(), True),
        StructField("writers", StringType(), True)
    ]),
    "title.principals": StructType([
        StructField("tconst", StringType(), True),
        StructField("ordering", StringType(), True), 
        StructField("nconst", StringType(), True),
        StructField("category", StringType(), True),
        StructField("job", StringType(), True),
        StructField("characters", StringType(), True)
    ]),
    "name.basics": StructType([
        StructField("nconst", StringType(), True),
        StructField("primaryName", StringType(), True),
        StructField("birthYear", StringType(), True), 
        StructField("deathYear", StringType(), True), 
        StructField("primaryProfession", StringType(), True),
        StructField("knownForTitles", StringType(), True)
    ])
}
# src/data_curated.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, expr, array_contains, lit, when, array_distinct, concat_ws, trim
from src.config import RAW_ZONE_PATH, CURATED_ZONE_PATH, REGION_OF_INTEREST

def transform_to_curated_zone(spark: SparkSession):
  
    os.makedirs(CURATED_ZONE_PATH, exist_ok=True)

    TARGET_REGIONS_FOR_CURATION = ["US", "AR", "ES", "FR", REGION_OF_INTEREST]

    try:
        df_basics = spark.read.parquet(os.path.join(RAW_ZONE_PATH, "title_basics"))
        df_akas = spark.read.parquet(os.path.join(RAW_ZONE_PATH, "title_akas"))
        df_ratings = spark.read.parquet(os.path.join(RAW_ZONE_PATH, "title_ratings"))
        df_crew = spark.read.parquet(os.path.join(RAW_ZONE_PATH, "title_crew"))
        df_principals = spark.read.parquet(os.path.join(RAW_ZONE_PATH, "title_principals"))
        df_names = spark.read.parquet(os.path.join(RAW_ZONE_PATH, "name_basics"))
    except Exception as e:
        print(f"Error al cargar archivos Parquet de Raw Zone. Error: {e}")
        raise

 
    
    df_movies_series = df_basics.filter(
        (col("titleType") == "movie") |
        (col("titleType") == "tvSeries") |
        (col("titleType") == "tvMiniSeries") |
        (col("titleType") == "tvMovie") 
    ).select(
        "tconst", "primaryTitle", "originalTitle", "startYear", "runtimeMinutes", "genres", "isAdult"
    )
    print(f"DataFrame de películas y series cargado con {df_movies_series.count()} registros.")
   


    df_regional_akas = df_akas.filter(
        (col("region").isin(TARGET_REGIONS_FOR_CURATION)) & 
        (col("isOriginalTitle") == False) &
        (col("title").isNotNull()) & (trim(col("title")) != '') 
    ).select(
        col("titleId").alias("tconst_akas"),
        col("title").alias("regional_title_from_akas"), 
        "region", 
        "language" 
    ).dropDuplicates(["tconst_akas", "region"]) 

    print(f"DataFrame de títulos regionales cargado con {df_regional_akas.count()} registros.")
    



    df_movies_with_akas = df_movies_series.join(
        df_regional_akas,
        on=df_movies_series.tconst == df_regional_akas.tconst_akas,
        how="right_outer"
    ).withColumn(
        "regional_title", 
        when(col("regional_title_from_akas").isNotNull(), col("regional_title_from_akas"))
        .otherwise(col("primaryTitle")) 
    ).drop("regional_title_from_akas", "tconst_akas") 

    print(f"DataFrame de películas con títulos regionales cargado con {df_movies_with_akas.count()} registros.")    

   
    df_curated_with_ratings = df_movies_with_akas.join(df_ratings, on="tconst", how="left") \
                                                .select(
                                                    col("tconst"),
                                                    col("regional_title"),
                                                    col("primaryTitle").alias("global_primary_title"),
                                                    col("originalTitle").alias("global_original_title"),
                                                    col("startYear"),
                                                    col("runtimeMinutes"),
                                                    col("genres"),
                                                    col("isAdult"),
                                                    col("averageRating"),
                                                    col("numVotes"),
                                                    "region", 
                                                    "language"
                                                )

    print(f"DataFrame curado con ratings cargado con {df_curated_with_ratings.count()} registros.")
    grouping_keys = [
        "tconst", "regional_title", "global_primary_title",
        "global_original_title", "startYear", "runtimeMinutes", "genres",
        "isAdult", "averageRating", "numVotes", "region", "language"
    ]
    unique_count_before_directors = df_curated_with_ratings.select(*grouping_keys).distinct().count()
    print(f"Número de registros únicos por claves de agrupación en df_curated_with_ratings: {unique_count_before_directors}")
   
   #separa el array de directores y te genera las filas para cdaa director
    df_crew_exploded = df_crew.withColumn("director_nconst", explode(split(col("directors"), ","))) \
                               .filter(col("director_nconst").isNotNull() & (col("director_nconst") != ''))

    df_curated_with_directors = df_curated_with_ratings.join(
        df_crew_exploded, on="tconst", how="left"
    ).join(
        df_names.select(col("nconst").alias("director_person_nconst"), col("primaryName").alias("director_name")),
        col("director_nconst") == col("director_person_nconst"),
        how="left"
    ).groupBy(
        "tconst", "regional_title", "global_primary_title",
        "global_original_title", "startYear", "runtimeMinutes", "genres",
        "isAdult", "averageRating", "numVotes","region", "language"
    ).agg(
        array_distinct(expr("collect_list(director_name)")).alias("directors")
    )

    print(f"DataFrame curado con directores cargado con {df_curated_with_directors.count()} registros.")

    
    df_principals_filtered = df_principals.filter(
        (col("category") == "actor") | (col("category") == "actress") | (col("category") == "self")
    )

    df_actors = df_principals_filtered.join(
        df_names.select(col("nconst").alias("actor_person_nconst"), col("primaryName").alias("actor_name")),
        df_principals_filtered.nconst == col("actor_person_nconst"),
        how="inner"
    ).groupBy("tconst").agg(
        array_distinct(expr("collect_list(actor_name)")).alias("principal_actors")
    )

    print(f"DataFrame curado con actores cargado con {df_actors.count()} registros.")

    df_final_curated = df_curated_with_directors.join(df_actors, on="tconst", how="left")

    print(f"DataFrame final curado con actores y directores cargado con {df_final_curated.count()} registros.")
    df_final_curated = df_final_curated.select(
        col("tconst"),
        col("regional_title"),
        col("global_primary_title"),
        col("global_original_title"),
        col("startYear"),
        col("runtimeMinutes"),
        col("genres"),
        col("isAdult"),
        col("averageRating"),
        col("numVotes"),
        col("directors"), 
        col("principal_actors"),
        col("region"),
        col("language")
    )

    print(f"DataFrame final curado con las columnas seleccionadas cargado con {df_final_curated.count()} registros.")

    df_final_curated.write.parquet(os.path.join(CURATED_ZONE_PATH, "regional_movies_data"), mode="overwrite")
    print(f"Dataset curado para las regiones '{', '.join(TARGET_REGIONS_FOR_CURATION)}' creado en Curated Zone.")
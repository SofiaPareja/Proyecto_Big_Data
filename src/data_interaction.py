# src/data_product.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, count, avg, sum,trim
from pyspark.sql.types import IntegerType
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from src.config import CURATED_ZONE_PATH, REPORTS_DIR, REGION_OF_INTEREST
import datetime

def generate_kpis_and_visualizations(spark: SparkSession):
    
    TARGET_COUNTRIES = ["US", "AR", "ES", "FR"]
    
    os.makedirs(REPORTS_DIR, exist_ok=True)

    try:
        df_curated = spark.read.parquet(os.path.join(CURATED_ZONE_PATH, "regional_movies_data"))
    except Exception as e:
        print(f"Error al cargar el archivo Parquet curado.Error: {e}")
        return 
    
    print(f"DataFrame curado cargado con {df_curated.count()} registros ")

    if 'region' in df_curated.columns:
        df_curated_uy_spark = df_curated.filter(col("region") == REGION_OF_INTEREST)
        
        print(f"DataFrame Spark filtrado para la región '{REGION_OF_INTEREST}' ('UY') creado.")
        print(f"cantida de registro sfiltrado {df_curated_uy_spark.count()} registros.")
    else:
        print("Advertencia: La columna 'region' no se encontró en df_curated. Los KPIs específicos de región no se filtrarán correctamente.")
        df_curated_uy_spark = df_curated
    
    # try:
        
    #     df_pd = df_curated.select(
    #         "tconst", "regional_title", "global_primary_title", "startYear",
    #         "genres", "averageRating", "numVotes", "directors", "principal_actors"
    #     ).toPandas()
    # except Exception as e:
    #     df_pd = df_curated.limit(100000).toPandas() #agarrar una muestra si es muy grande


    #KPI 1: top 10 generos mas populares por número de películas y rating promedio en region UY
    # if 'genres' in df_pd.columns and not df_pd['genres'].isnull().all():
    #     df_genres_exploded = df_pd.assign(genre=df_pd['genres'].apply(lambda x: str(x).split(',') if x else [])) \
    #                               .explode('genre')
    #     df_genres_exploded['genre'] = df_genres_exploded['genre'].str.strip()
        
    #     genre_popularity = df_genres_exploded[df_genres_exploded['genre'] != ''].groupby("genre").agg(
    #         count=('tconst', 'count'),
    #         avg_rating=('averageRating', 'mean'),
    #         total_votes=('numVotes', 'sum')
    #     ).reset_index().sort_values(by="count", ascending=False).head(10)

    #     plt.figure(figsize=(14, 7))
    #     sns.barplot(x="genre", y="count", data=genre_popularity, palette="viridis")
    #     plt.title(f"Top 10 generos mas populares por numero de pelis en {REGION_OF_INTEREST}", fontsize=16)
    #     plt.xlabel("genero", fontsize=12)
    #     plt.ylabel("numro de peliculas", fontsize=12)
    #     plt.xticks(rotation=45, ha='right', fontsize=10)
    #     plt.yticks(fontsize=10)
    #     plt.tight_layout()
    #     plt.savefig(os.path.join(REPORTS_DIR, "genre_popularity.png"))
    #     plt.close()
        
    # else:
    #     print("no se pudo generar el KPI de generos")

    if 'genres' in df_curated_uy_spark.columns and 'tconst' in df_curated_uy_spark.columns:
        df_genres_exploded_uy_spark = df_curated_uy_spark.filter(
            col("genres").isNotNull() & (trim(col("genres")) != '')
        ).withColumn("genre", explode(split(col("genres"), ","))) \
         .withColumn("genre", trim(col("genre")))
        
        genre_popularity_uy_spark = df_genres_exploded_uy_spark.filter(col("genre") != '').groupBy("genre").agg(
            count(col("tconst")).alias("count"),
            avg(col("averageRating")).alias("avg_rating"),
            sum(col("numVotes")).alias("total_votes")
        ).orderBy(col("count").desc()).limit(10)

        if not genre_popularity_uy_spark.isEmpty():
            genre_popularity_uy_pd = genre_popularity_uy_spark.toPandas()

            plt.figure(figsize=(14, 7))
            sns.barplot(x="genre", y="count", data=genre_popularity_uy_pd, palette="viridis")
            plt.title(f"Top 10 géneros más populares por número de películas en {REGION_OF_INTEREST}", fontsize=16)
            plt.xlabel("Género", fontsize=12)
            plt.ylabel("Número de películas", fontsize=12)
            plt.xticks(rotation=45, ha='right', fontsize=10)
            plt.yticks(fontsize=10)
            plt.tight_layout()
            plt.savefig(os.path.join(REPORTS_DIR, f"genre_popularity_{REGION_OF_INTEREST}.png"))
            plt.close()
        else:
            print(f"No se pudo generar el KPI de géneros para {REGION_OF_INTEREST}: el DataFrame de popularidad de géneros está vacío.")
    else:
        print(f"No se pudo generar el KPI de géneros para {REGION_OF_INTEREST}: columna 'genres' o 'tconst' no disponible en el DataFrame curado filtrado.")


    #KPI 2: Top películas prometedoras por rating y votos ---
    # Consideramos "prometedora" por una combinación de rating alto y un mínimo de votos
    # min_votes_limite = 1000 
    # if 'numVotes' in df_pd.columns and 'averageRating' in df_pd.columns:
    #     df_promising_movies = df_pd[(df_pd['numVotes'] >= min_votes_limite) & (df_pd['averageRating'].notna())]
        
    #     if not df_promising_movies.empty:
    #         top_promising = df_promising_movies.sort_values(by=['averageRating', 'numVotes'], ascending=[False, False]).head(20)
    #         top_promising['directors_str'] = top_promising['directors'].apply(lambda x: ', '.join(x) if isinstance(x, list) else '')
    #         top_promising['principal_actors_str'] = top_promising['principal_actors'].apply(lambda x: ', '.join(x) if isinstance(x, list) else '')

    #         print(top_promising[[
    #             'regional_title', 'startYear', 'genres', 'averageRating', 'numVotes',
    #             'directors_str', 'principal_actors_str'
    #         ]].to_string(index=False))
    #     else:
    #         print(f"No se encontraron películas que cumplan con los criterios de minimo de votos {min_votes_limite} en  region{REGION_OF_INTEREST}.")
    # else:
    #     print("No se pudo generar el KPI de películas prometedoras")



    min_votes_limite = 1000 
    #usamos df_curated_uy_spark para este KPI
    if all(col_name in df_curated_uy_spark.columns for col_name in ['numVotes', 'averageRating', 'directors', 'principal_actors', 'regional_title', 'startYear', 'genres']):
        df_promising_movies_uy_spark = df_curated_uy_spark.filter(
            (col('numVotes') >= min_votes_limite) & (col('averageRating').isNotNull())
        ).orderBy(col('averageRating').desc(), col('numVotes').desc()).limit(20)

        if not df_promising_movies_uy_spark.isEmpty():
            df_promising_movies_uy_pd = df_promising_movies_uy_spark.select(
                "regional_title", "startYear", "genres", "averageRating", "numVotes",
                "directors", "principal_actors"
            ).toPandas()
            
            df_promising_movies_uy_pd['directors_str'] = df_promising_movies_uy_pd['directors'].apply(lambda x: ', '.join(x) if isinstance(x, list) else '')
            df_promising_movies_uy_pd['principal_actors_str'] = df_promising_movies_uy_pd['principal_actors'].apply(lambda x: ', '.join(x) if isinstance(x, list) else '')

            print(f"\nTop Películas Prometedoras en {REGION_OF_INTEREST}")
            print(df_promising_movies_uy_pd[[
                'regional_title', 'startYear', 'genres', 'averageRating', 'numVotes',
                'directors_str', 'principal_actors_str'
            ]].to_string(index=False))
        else:
            print(f"No se encontraron películas que cumplan con los criterios de mínimo de votos {min_votes_limite} en {REGION_OF_INTEREST}.")
    else:
        print(f"No se pudo generar el KPI de películas prometedoras para {REGION_OF_INTEREST}: columnas necesarias no disponibles en el DataFrame curado filtrado.")


    # #KPI 3: distribución de ratings
    # if 'averageRating' in df_pd.columns and not df_pd['averageRating'].isnull().all():
    #     plt.figure(figsize=(10, 6))
    #     sns.histplot(df_pd['averageRating'].dropna(), bins=20, kde=True, color='skyblue')
    #     plt.title(f"distribución de ratings en {REGION_OF_INTEREST}", fontsize=16)
    #     plt.xlabel("rating promedio", fontsize=12)
    #     plt.ylabel("frecuencia", fontsize=12)
    #     plt.xticks(fontsize=10)
    #     plt.yticks(fontsize=10)
    #     plt.tight_layout()
    #     plt.savefig(os.path.join(REPORTS_DIR, "rating_distribution.png"))
    #     plt.close()
    #     print(f"Reporte: Distribución de Ratings guardado en {os.path.join(REPORTS_DIR, 'rating_distribution.png')}")
    # else:
    #     print("No se pudieron generar el KPI de distribución de ratings: columna 'averageRating' no disponible o vacía.")

   
    # if 'startYear' in df_curated.columns and 'genres' in df_curated.columns:
    #     df_exploded_genres_by_year_spark = df_curated.filter(
    #         col("startYear").isNotNull() & col("genres").isNotNull() & (trim(col("genres")) != '')
    #     ) \
    #     .withColumn("startYear_int", col("startYear").cast(IntegerType())) \
    #     .withColumn("genre", explode(split(col("genres"), ","))) \
    #     .withColumn("genre", trim(col("genre")))

    #     df_exploded_genres_by_year_spark = df_exploded_genres_by_year_spark.filter(col("genre") != '')

    #     # (Opcional) Limitar a los Top N géneros más frecuentes para la visualización
    #     # Esto hace el gráfico más legible si hay muchos géneros. Aquí tomamos los 7 principales.
    #     NUM_TOP_GENRES_FOR_TRENDS = 7 # Puedes ajustar este número
        
    #     # Primero, calcula la frecuencia de los géneros en todo el dataset curado
    #     genre_counts_overall = df_exploded_genres_by_year_spark.groupBy("genre").count().orderBy(col("count").desc())
        
    #     # Obtiene los nombres de los géneros más frecuentes
    #     top_genres_names = [row.genre for row in genre_counts_overall.limit(NUM_TOP_GENRES_FOR_TRENDS).collect()]
        
    #     # Filtra el DataFrame explotado para incluir solo estos géneros principales
    #     df_filtered_genres_for_trends_spark = df_exploded_genres_by_year_spark.filter(col("genre").isin(top_genres_names))

    #     # 2. Agrupar por año y género, y contar películas con Spark
    #     year_genre_trends_spark = df_filtered_genres_for_trends_spark.groupBy("startYear_int", "genre") \
    #                                                                 .agg(count(col("tconst")).alias("count")) \
    #                                                                 .orderBy("startYear_int", "genre")

    #     # 3. Convertir el resultado pequeño y agregado a Pandas para la visualización
    #     if not year_genre_trends_spark.isEmpty():
    #         year_genre_trends_pd = year_genre_trends_spark.toPandas()

    #         plt.figure(figsize=(15, 8)) # Ajusta el tamaño de la figura
    #         sns.lineplot(x="startYear_int", y="count", hue="genre", data=year_genre_trends_pd, marker='o') # 'hue' para líneas por género
    #         plt.title(f"Número de Películas Estrenadas por Año por Género (Top {len(top_genres_names)} Géneros) en {REGION_OF_INTEREST}", fontsize=16)
    #         plt.xlabel("Año de Estreno", fontsize=12)
    #         plt.ylabel("Número de Películas", fontsize=12)
    #         plt.grid(True)
    #         plt.legend(title="Género", bbox_to_anchor=(1.05, 1), loc='upper left') # Mueve la leyenda fuera del gráfico
    #         plt.tight_layout()
    #         plt.savefig(os.path.join(REPORTS_DIR, "year_genre_trends.png")) # Guarda con un nuevo nombre
    #         plt.close()
    #         print(f"Reporte: Películas por Año de Estreno por Género guardado en {os.path.join(REPORTS_DIR, 'year_genre_trends.png')}")
    #     else:
    #         print("No se pudo generar el KPI de tendencias por año por género: el DataFrame de tendencias está vacío.")
        
    # else:
    #     print("No se pudo generar el KPI de tendencias por año por género: columna 'startYear' o 'genres' no disponible en el DataFrame curado.")



    #KPI 3: distribucion de ratings en region UY
    #usamos df_curated_uy_spark para este KPI
    if 'averageRating' in df_curated_uy_spark.columns:
        df_for_hist_uy_spark = df_curated_uy_spark.select("averageRating").filter(col("averageRating").isNotNull())

        if df_for_hist_uy_spark.count() > 0: 
            df_for_hist_uy_pd = df_for_hist_uy_spark.toPandas() 

            plt.figure(figsize=(10, 6))
            sns.histplot(df_for_hist_uy_pd['averageRating'].dropna(), bins=20, kde=True, color='skyblue')
            plt.title(f"Distribución de ratings en {REGION_OF_INTEREST}", fontsize=16)
            plt.xlabel("Rating promedio", fontsize=12)
            plt.ylabel("Frecuencia", fontsize=12)
            plt.xticks(fontsize=10)
            plt.yticks(fontsize=10)
            plt.tight_layout()
            plt.savefig(os.path.join(REPORTS_DIR, f"rating_distribution_{REGION_OF_INTEREST}.png"))
            plt.close()
        else:
            print(f"No se pudieron generar el KPI de distribución de ratings para {REGION_OF_INTEREST}: datos de 'averageRating' no disponibles o vacíos.")
    else:
        print(f"No se pudieron generar el KPI de distribución de ratings para {REGION_OF_INTEREST}: columna 'averageRating' no disponible.")

   
    #KPI 4: Número de películas estrenadas por año por genero en region UY
    # usamos df_curated_uy_spark para este KPI
    if 'startYear' in df_curated_uy_spark.columns and 'genres' in df_curated_uy_spark.columns:
        df_exploded_genres_by_year_uy_spark = df_curated_uy_spark.filter(
            col("startYear").isNotNull() & col("genres").isNotNull() & (trim(col("genres")) != '')
        ) \
        .withColumn("startYear_int", col("startYear").cast(IntegerType())) \
        .withColumn("genre", explode(split(col("genres"), ","))) \
        .withColumn("genre", trim(col("genre")))

        df_exploded_genres_by_year_uy_spark = df_exploded_genres_by_year_uy_spark.filter(col("genre") != '')

        NUM_TOP_GENRES_FOR_TRENDS = 7 
        
        genre_counts_uy = df_exploded_genres_by_year_uy_spark.groupBy("genre").count().orderBy(col("count").desc())
        top_genres_names_uy = [row.genre for row in genre_counts_uy.limit(NUM_TOP_GENRES_FOR_TRENDS).collect()]
        
        df_filtered_genres_for_trends_uy_spark = df_exploded_genres_by_year_uy_spark.filter(col("genre").isin(top_genres_names_uy))

        year_genre_trends_uy_spark = df_filtered_genres_for_trends_uy_spark.groupBy("startYear_int", "genre") \
                                                                    .agg(count(col("tconst")).alias("count")) \
                                                                    .orderBy("startYear_int", "genre")

        if not year_genre_trends_uy_spark.isEmpty():
            year_genre_trends_uy_pd = year_genre_trends_uy_spark.toPandas()

            plt.figure(figsize=(15, 8))
            sns.lineplot(x="startYear_int", y="count", hue="genre", data=year_genre_trends_uy_pd, marker='o')
            plt.title(f"Número de Películas Estrenadas por Año por Género (Top {len(top_genres_names_uy)} Géneros) en {REGION_OF_INTEREST}", fontsize=16)
            plt.xlabel("Año de Estreno", fontsize=12)
            plt.ylabel("Número de Películas", fontsize=12)
            plt.grid(True)
            plt.legend(title="Género", bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.tight_layout()
            plt.savefig(os.path.join(REPORTS_DIR, f"year_genre_trends_{REGION_OF_INTEREST}.png"))
            plt.close()
            print(f"Reporte: Películas por Año de Estreno por Género para {REGION_OF_INTEREST} guardado en {os.path.join(REPORTS_DIR, f'year_genre_trends_{REGION_OF_INTEREST}.png')}")
        else:
            print(f"No se pudo generar el KPI de tendencias por año por género para {REGION_OF_INTEREST}: el DataFrame de tendencias está vacío.")
        
    else:
        print(f"No se pudo generar el KPI de tendencias por año por género para {REGION_OF_INTEREST}: columna 'startYear' o 'genres' no disponible en el DataFrame curado filtrado.")

    #top 30 peliculas mas vistas en los ultimos 25 años por pais
    if 'startYear' in df_curated.columns and 'numVotes' in df_curated.columns and 'regional_title' in df_curated.columns and 'region' in df_curated.columns:
        current_year = 2025 
        start_year_threshold = current_year - 25

        print("\n--- Generando Top 30 Películas Más Vistas por País ---")
        for country_code in TARGET_COUNTRIES:
            print(f"Procesando Top 30 para {country_code}...")
            df_country_movies = df_curated.filter(
                (col("region") == country_code) &
                (col("startYear").isNotNull()) & (col("startYear").cast(IntegerType()) >= start_year_threshold) &
                (col("numVotes").isNotNull()) & (col("numVotes") >= min_votes_limite) 
            ) \
            .orderBy(col("numVotes").desc(), col("averageRating").desc()) \
            .limit(30)

            if not df_country_movies.isEmpty():
                df_country_movies_pd = df_country_movies.select("regional_title", "startYear", "numVotes", "averageRating").toPandas()
                
                plt.figure(figsize=(12, 10))
                sns.barplot(x="numVotes", y="regional_title", data=df_country_movies_pd, palette="magma")
                plt.title(f"Top 30 Películas Más Vistas en {country_code} (Últimos 25 Años)", fontsize=16)
                plt.xlabel("Número de Votos", fontsize=12)
                plt.ylabel("Título de la Película", fontsize=12)
                plt.xticks(fontsize=10)
                plt.yticks(fontsize=8) 
                plt.tight_layout()
                plt.savefig(os.path.join(REPORTS_DIR, f"top_30_movies_{country_code}.png"))
                plt.close()
            else:
                print(f"No se encontraron películas para el Top 30 en {country_code} que cumplan los criterios.")
    else:
        print("No se pudo generar el KPI de Top 30 Películas: columnas necesarias ('startYear', 'numVotes', 'regional_title', 'region') no disponibles.")


    #ano actual
    current_year = datetime.datetime.now().year 
    start_year_threshold = current_year - 25

    #top 30 peliculas mas vistas en los ultimos 25 años por pais
    #usamos df_curated (el df completo) y filtramos por cada pais
    if 'startYear' in df_curated.columns and 'numVotes' in df_curated.columns and 'regional_title' in df_curated.columns and 'region' in df_curated.columns:
        for country_code in TARGET_COUNTRIES:
            df_country_movies = df_curated.filter(
                (col("region") == country_code) & 
                (col("startYear").isNotNull()) & (col("startYear").cast(IntegerType()) >= start_year_threshold) &
                (col("numVotes").isNotNull()) & (col("numVotes") >= min_votes_limite) 
            ) \
            .orderBy(col("numVotes").desc(), col("averageRating").desc()) \
            .limit(30)

            if not df_country_movies.isEmpty():
                df_country_movies_pd = df_country_movies.select("regional_title", "startYear", "numVotes", "averageRating").toPandas()
                
                plt.figure(figsize=(12, 10)) 
                sns.barplot(x="numVotes", y="regional_title", data=df_country_movies_pd, palette="magma")
                plt.title(f"Top 30 Películas Más Vistas en {country_code} (Últimos 25 Años)", fontsize=16)
                plt.xlabel("Número de Votos", fontsize=12)
                plt.ylabel("Título de la Película", fontsize=12)
                plt.xticks(fontsize=10)
                plt.yticks(fontsize=8) 
                plt.tight_layout()
                plt.savefig(os.path.join(REPORTS_DIR, f"top_30_movies_{country_code}.png"))
                plt.close()
            else:
                print(f"No se encontraron películas para el Top 30 en {country_code} que cumplan los criterios.")
    else:
        print("No se pudo generar el KPI de Top 30 Películas por país: columnas necesarias ('startYear', 'numVotes', 'regional_title', 'region') no disponibles.")


    #tendencias de peluculas por año y genero, por pais
    #usamos df_curated (el df completo) y filtramos por cada pais
    if 'startYear' in df_curated.columns and 'genres' in df_curated.columns and 'region' in df_curated.columns:
        for country_code in TARGET_COUNTRIES:
            df_country_exploded_genres_by_year_spark = df_curated.filter(
                (col("region") == country_code) & 
                (col("startYear").isNotNull()) & (col("genres").isNotNull()) & (trim(col("genres")) != '')
            ) \
            .withColumn("startYear_int", col("startYear").cast(IntegerType())) \
            .withColumn("genre", explode(split(col("genres"), ","))) \
            .withColumn("genre", trim(col("genre")))

            df_country_exploded_genres_by_year_spark = df_country_exploded_genres_by_year_spark.filter(col("genre") != '')

            NUM_TOP_GENRES_FOR_TRENDS = 7 
            
            genre_counts_country = df_country_exploded_genres_by_year_spark.groupBy("genre").count().orderBy(col("count").desc())
            top_genres_names_country = [row.genre for row in genre_counts_country.limit(NUM_TOP_GENRES_FOR_TRENDS).collect()]
            
            df_filtered_genres_for_trends_country_spark = df_country_exploded_genres_by_year_spark.filter(col("genre").isin(top_genres_names_country))

            year_genre_trends_country_spark = df_filtered_genres_for_trends_country_spark.groupBy("startYear_int", "genre") \
                                                                                        .agg(count(col("tconst")).alias("count")) \
                                                                                        .orderBy("startYear_int", "genre")

            if not year_genre_trends_country_spark.isEmpty():
                year_genre_trends_country_pd = year_genre_trends_country_spark.toPandas()

                plt.figure(figsize=(15, 8))
                sns.lineplot(x="startYear_int", y="count", hue="genre", data=year_genre_trends_country_pd, marker='o')
                plt.title(f"Número de Películas Estrenadas por Año por Género (Top {len(top_genres_names_country)} Géneros) en {country_code}", fontsize=16)
                plt.xlabel("Año de Estreno", fontsize=12)
                plt.ylabel("Número de Películas", fontsize=12)
                plt.grid(True)
                plt.legend(title="Género", bbox_to_anchor=(1.05, 1), loc='upper left')
                plt.tight_layout()
                plt.savefig(os.path.join(REPORTS_DIR, f"year_genre_trends_{country_code}.png"))
                plt.close()
            else:
                print(f"No se pudo generar el KPI de tendencias por año por género para {country_code}: el DataFrame de tendencias está vacío.")
    else:
        print("No se pudo generar el KPI de tendencias por año por género por país: columnas necesarias ('startYear', 'genres', 'region') no disponibles.")





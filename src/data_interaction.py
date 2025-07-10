# src/data_product.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, count, avg, sum,trim
from pyspark.sql.types import IntegerType
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
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
            plt.title(f"Top 10 géneros más populares por cantidad de películas en {REGION_OF_INTEREST}", fontsize=16)
            plt.xlabel("Género", fontsize=12)
            plt.ylabel("Cantidad de películas", fontsize=12)
            plt.xticks(rotation=45, ha='right', fontsize=10)
            plt.yticks(fontsize=10)
            plt.tight_layout()
            plt.savefig(os.path.join(REPORTS_DIR, f"genre_popularity_{REGION_OF_INTEREST}.png"))
            plt.close()
        else:
            print(f"No se pudo generar el KPI de géneros para {REGION_OF_INTEREST}: el DataFrame de popularidad de géneros está vacío.")
    else:
        print(f"No se pudo generar el KPI de géneros para {REGION_OF_INTEREST}: columna 'genres' o 'tconst' no disponible en el DataFrame curado filtrado.")


    min_votes_limite = 1000 
   

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
            plt.title(f"Número de películas estrenadas por año por género (Top {len(top_genres_names_uy)} Géneros) en {REGION_OF_INTEREST}", fontsize=16)
            plt.xlabel("Año de estreno", fontsize=12)
            plt.ylabel("Número de películas", fontsize=12)
            plt.grid(True)
            plt.legend(title="Género", bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.tight_layout()
            plt.savefig(os.path.join(REPORTS_DIR, f"year_genre_trends_{REGION_OF_INTEREST}.png"))
            plt.close()
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
                plt.title(f"Top 30 películas mas vistas en {country_code} (ultimos 25 años)", fontsize=16)
                plt.xlabel("Número de votos", fontsize=12)
                plt.ylabel("Título de película", fontsize=12)
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
                plt.title(f"Top 30 películas mas vistas en {country_code} (ultimos 25 años)", fontsize=16)
                plt.xlabel("Número de votos", fontsize=12)
                plt.ylabel("Título de  película", fontsize=12)
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
                plt.title(f"Número de películas estrenadas por año por género (Top {len(top_genres_names_country)} Géneros) en {country_code}", fontsize=16)
                plt.xlabel("Año de eestreno", fontsize=12)
                plt.ylabel("Número de películas", fontsize=12)
                plt.grid(True)
                plt.legend(title="Género", bbox_to_anchor=(1.05, 1), loc='upper left')
                plt.tight_layout()
                plt.savefig(os.path.join(REPORTS_DIR, f"year_genre_trends_{country_code}.png"))
                plt.close()
            else:
                print(f"No se pudo generar el KPI de tendencias por año por género para {country_code}: el DataFrame de tendencias está vacío.")
    else:
        print("No se pudo generar el KPI de tendencias por año por género por país: columnas necesarias ('startYear', 'genres', 'region') no disponibles.")

  
    if 'runtimeMinutes' in df_curated_uy_spark.columns:
        df_runtime_uy = df_curated_uy_spark.select('runtimeMinutes').filter(col('runtimeMinutes').isNotNull())
        if df_runtime_uy.count() > 0:
            df_runtime_uy_pd = df_runtime_uy.toPandas()
            plt.figure(figsize=(10, 6))
            sns.histplot(df_runtime_uy_pd['runtimeMinutes'], bins=30, kde=True, color='orange')
            plt.title(f"Distribucion de duración de películas en {REGION_OF_INTEREST}", fontsize=16)
            plt.xlabel("Duración en min", fontsize=12)
            plt.ylabel("Frecuencia", fontsize=12)
            plt.tight_layout()
            plt.savefig(os.path.join(REPORTS_DIR, f"runtime_distribution_{REGION_OF_INTEREST}.png"))
            plt.close()

    if 'genres' in df_curated_uy_spark.columns and 'averageRating' in df_curated_uy_spark.columns:
        df_genre_rating_uy = df_curated_uy_spark.filter(col('genres').isNotNull() & (trim(col('genres')) != '')) \
            .withColumn('genre', explode(split(col('genres'), ','))) \
            .withColumn('genre', trim(col('genre')))
        genre_rating_uy = df_genre_rating_uy.groupBy('genre').agg(avg('averageRating').alias('avg_rating')).orderBy(col('avg_rating').desc())
        if not genre_rating_uy.isEmpty():
            genre_rating_uy_pd = genre_rating_uy.toPandas()
            plt.figure(figsize=(14, 7))
            sns.barplot(x='avg_rating', y='genre', data=genre_rating_uy_pd, palette='coolwarm')
            plt.title(f"Rating promedio por gnrero en {REGION_OF_INTEREST}", fontsize=16)
            plt.xlabel("Rating promedio", fontsize=12)
            plt.ylabel("Género", fontsize=12)
            plt.tight_layout()
            plt.savefig(os.path.join(REPORTS_DIR, f"avg_rating_by_genre_{REGION_OF_INTEREST}.png"))
            plt.close()


    if 'numVotes' in df_curated_uy_spark.columns and 'averageRating' in df_curated_uy_spark.columns:
        df_votes_rating_uy = df_curated_uy_spark.select('numVotes', 'averageRating').filter(col('numVotes').isNotNull() & col('averageRating').isNotNull())
        if df_votes_rating_uy.count() > 0:
            df_votes_rating_uy_pd = df_votes_rating_uy.toPandas()
            plt.figure(figsize=(10, 6))
            sns.scatterplot(x='numVotes', y='averageRating', data=df_votes_rating_uy_pd, alpha=0.5)
            plt.title(f"Correlacion entre num de votos y rating en {REGION_OF_INTEREST}", fontsize=16)
            plt.xlabel("Número de votos", fontsize=12)
            plt.ylabel("Rating promedio", fontsize=12)
            plt.tight_layout()
            plt.savefig(os.path.join(REPORTS_DIR, f"votes_vs_rating_{REGION_OF_INTEREST}.png"))
            plt.close()

   

    if 'directors' in df_curated_uy_spark.columns:
        df_directors_uy = df_curated_uy_spark.select('directors').filter(col('directors').isNotNull())
        df_directors_uy_pd = df_directors_uy.toPandas()
        all_directors = df_directors_uy_pd['directors'].explode()
        top_directors = all_directors.value_counts().head(10)
        plt.figure(figsize=(12, 7))
        sns.barplot(x=top_directors.values, y=top_directors.index, palette='Blues_r')
        plt.title(f"Top 10 directores por cant de películas en {REGION_OF_INTEREST}", fontsize=16)
        plt.xlabel("Cantidad de Películas", fontsize=12)
        plt.ylabel("Director", fontsize=12)
        plt.tight_layout()
        plt.savefig(os.path.join(REPORTS_DIR, f"top_directors_{REGION_OF_INTEREST}.png"))
        plt.close()

    if 'principal_actors' in df_curated_uy_spark.columns:
        df_actors_uy = df_curated_uy_spark.select('principal_actors').filter(col('principal_actors').isNotNull())
        df_actors_uy_pd = df_actors_uy.toPandas()
        all_actors = df_actors_uy_pd['principal_actors'].explode()
        top_actors = all_actors.value_counts().head(10)
        plt.figure(figsize=(12, 7))
        sns.barplot(x=top_actors.values, y=top_actors.index, palette='Greens_r')
        plt.title(f"Top 10 actores/actrices por número de peliculas en {REGION_OF_INTEREST}", fontsize=16)
        plt.xlabel("Cantidad de Películas", fontsize=12)
        plt.ylabel("Actor/actriz", fontsize=12)
        plt.tight_layout()
        plt.savefig(os.path.join(REPORTS_DIR, f"top_actors_{REGION_OF_INTEREST}.png"))
        plt.close()


    if all(col in df_curated_uy_spark.columns for col in ['averageRating', 'numVotes', 'regional_title', 'startYear']):
        df_revenue_potential_uy = df_curated_uy_spark.filter(
            col('averageRating').isNotNull() & col('numVotes').isNotNull()
        ).withColumn('revenue_potential', col('averageRating') * col('numVotes'))
        
        top_revenue_uy = df_revenue_potential_uy.orderBy(col('revenue_potential').desc()).limit(15)
        if not top_revenue_uy.isEmpty():
            top_revenue_uy_pd = top_revenue_uy.select('regional_title', 'startYear', 'revenue_potential', 'averageRating', 'numVotes').toPandas()
            
            plt.figure(figsize=(14, 8))
            sns.barplot(x='revenue_potential', y='regional_title', data=top_revenue_uy_pd, palette='viridis')
            plt.title(f"Top 15 películas vista en uruguay por potencial de ingresos (rating por votos)", fontsize=16)
            plt.xlabel("Potencial de ingresos", fontsize=12)
            plt.ylabel("Titulo de la película", fontsize=10)
            plt.tight_layout()
            plt.savefig(os.path.join(REPORTS_DIR, f"top_revenue_potential_{REGION_OF_INTEREST}.png"))
            plt.close()

    if all(col in df_curated_uy_spark.columns for col in ['genres', 'startYear']):
        current_year = 2025
        df_genre_trends_uy = df_curated_uy_spark.filter(
            col('startYear').isNotNull() & 
            (col('startYear').cast(IntegerType()) >= current_year - 20) &
            col('genres').isNotNull()
        ).withColumn('startYear_int', col('startYear').cast(IntegerType())) \
         .withColumn('genre', explode(split(col('genres'), ','))) \
         .withColumn('genre', trim(col('genre')))
        
        genre_trends_uy = df_genre_trends_uy.groupBy('startYear_int', 'genre').count().orderBy('startYear_int', 'genre')
        if not genre_trends_uy.isEmpty():
            genre_trends_uy_pd = genre_trends_uy.toPandas()
            
            top_genres = genre_trends_uy_pd.groupby('genre')['count'].sum().nlargest(5).index
            genre_trends_filtered = genre_trends_uy_pd[genre_trends_uy_pd['genre'].isin(top_genres)]
            
            plt.figure(figsize=(15, 8))
            for genre in top_genres:
                genre_data = genre_trends_filtered[genre_trends_filtered['genre'] == genre]
                plt.plot(genre_data['startYear_int'], genre_data['count'], marker='o', label=genre, linewidth=2)
            
            plt.title(f"Tendencias de popularidad de géneros en Uruguay (ultimos 20 años)", fontsize=16)
            plt.xlabel("Año", fontsize=12)
            plt.ylabel("Número de Películas", fontsize=12)
            plt.legend(title="Género", bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(os.path.join(REPORTS_DIR, f"genre_trends_20years_{REGION_OF_INTEREST}.png"))
            plt.close()

    if 'runtimeMinutes' in df_curated_uy_spark.columns:
        df_runtime_uy = df_curated_uy_spark.select('runtimeMinutes').filter(col('runtimeMinutes').isNotNull())
        if df_runtime_uy.count() > 0:
            df_runtime_uy_pd = df_runtime_uy.toPandas()
            plt.figure(figsize=(10, 6))
            sns.histplot(df_runtime_uy_pd['runtimeMinutes'], bins=30, kde=True, color='orange')
            plt.title(f"Distribución de duracion de películas en {REGION_OF_INTEREST}", fontsize=16)
            plt.xlabel("Duración en min", fontsize=12)
            plt.ylabel("Frecuencia", fontsize=12)
            plt.tight_layout()
            plt.savefig(os.path.join(REPORTS_DIR, f"runtime_distribution_{REGION_OF_INTEREST}.png"))
            plt.close()


    if 'genres' in df_curated_uy_spark.columns and 'averageRating' in df_curated_uy_spark.columns:
        df_genre_rating_uy = df_curated_uy_spark.filter(col('genres').isNotNull() & (trim(col('genres')) != '')) \
            .withColumn('genre', explode(split(col('genres'), ','))) \
            .withColumn('genre', trim(col('genre')))
        genre_rating_uy = df_genre_rating_uy.groupBy('genre').agg(avg('averageRating').alias('avg_rating')).orderBy(col('avg_rating').desc())
        if not genre_rating_uy.isEmpty():
            genre_rating_uy_pd = genre_rating_uy.toPandas()
            plt.figure(figsize=(14, 7))
            sns.barplot(x='avg_rating', y='genre', data=genre_rating_uy_pd, palette='coolwarm')
            plt.title(f"Rating promedio por género en {REGION_OF_INTEREST}", fontsize=16)
            plt.xlabel("Rating promedio", fontsize=12)
            plt.ylabel("Género", fontsize=12)
            plt.tight_layout()
            plt.savefig(os.path.join(REPORTS_DIR, f"avg_rating_by_genre_{REGION_OF_INTEREST}.png"))
            plt.close()

    if 'numVotes' in df_curated_uy_spark.columns and 'averageRating' in df_curated_uy_spark.columns:
        df_votes_rating_uy = df_curated_uy_spark.select('numVotes', 'averageRating').filter(col('numVotes').isNotNull() & col('averageRating').isNotNull())
        if df_votes_rating_uy.count() > 0:
            df_votes_rating_uy_pd = df_votes_rating_uy.toPandas()
            plt.figure(figsize=(10, 6))
            sns.scatterplot(x='numVotes', y='averageRating', data=df_votes_rating_uy_pd, alpha=0.5)
            plt.title(f"Correlación entre num de votos y rating en {REGION_OF_INTEREST}", fontsize=16)
            plt.xlabel("Número de votos", fontsize=12)
            plt.ylabel("Rating promedio", fontsize=12)
            plt.tight_layout()
            plt.savefig(os.path.join(REPORTS_DIR, f"votes_vs_rating_{REGION_OF_INTEREST}.png"))
            plt.close()



    if 'directors' in df_curated_uy_spark.columns:
        df_directors_uy = df_curated_uy_spark.select('directors').filter(col('directors').isNotNull())
        df_directors_uy_pd = df_directors_uy.toPandas()
        all_directors = df_directors_uy_pd['directors'].explode()
        top_directors = all_directors.value_counts().head(10)
        plt.figure(figsize=(12, 7))
        sns.barplot(x=top_directors.values, y=top_directors.index, palette='Blues_r')
        plt.title(f"Top 10 directores por numero de películas en {REGION_OF_INTEREST}", fontsize=16)
        plt.xlabel("Cantidad de películas", fontsize=12)
        plt.ylabel("Director", fontsize=12)
        plt.tight_layout()
        plt.savefig(os.path.join(REPORTS_DIR, f"top_directors_{REGION_OF_INTEREST}.png"))
        plt.close()


    if 'principal_actors' in df_curated_uy_spark.columns:
        df_actors_uy = df_curated_uy_spark.select('principal_actors').filter(col('principal_actors').isNotNull())
        df_actors_uy_pd = df_actors_uy.toPandas()
        all_actors = df_actors_uy_pd['principal_actors'].explode()
        top_actors = all_actors.value_counts().head(10)
        plt.figure(figsize=(12, 7))
        sns.barplot(x=top_actors.values, y=top_actors.index, palette='Greens_r')
        plt.title(f"Top 10 actores/actrices por número de películas en {REGION_OF_INTEREST}", fontsize=16)
        plt.xlabel("Cantidad de películas", fontsize=12)
        plt.ylabel("actor/actriz", fontsize=12)
        plt.tight_layout()
        plt.savefig(os.path.join(REPORTS_DIR, f"top_actors_{REGION_OF_INTEREST}.png"))
        plt.close()

    
    if all(col in df_curated_uy_spark.columns for col in ['averageRating', 'numVotes', 'regional_title', 'startYear']):
        df_revenue_potential_uy = df_curated_uy_spark.filter(
            col('averageRating').isNotNull() & col('numVotes').isNotNull()
        ).withColumn('revenue_potential', col('averageRating') * col('numVotes'))
        
        top_revenue_uy = df_revenue_potential_uy.orderBy(col('revenue_potential').desc()).limit(15)
        if not top_revenue_uy.isEmpty():
            top_revenue_uy_pd = top_revenue_uy.select('regional_title', 'startYear', 'revenue_potential', 'averageRating', 'numVotes').toPandas()
            
            plt.figure(figsize=(14, 8))
            sns.barplot(x='revenue_potential', y='regional_title', data=top_revenue_uy_pd, palette='viridis')
            plt.title(f"Top 15 películas uruguayas por potencial de ingresos (rating por votos)", fontsize=16)
            plt.xlabel("Potencial de ingresos", fontsize=12)
            plt.ylabel("Título de la película", fontsize=10)
            plt.tight_layout()
            plt.savefig(os.path.join(REPORTS_DIR, f"top_revenue_potential_{REGION_OF_INTEREST}.png"))
            plt.close()

    if all(col in df_curated_uy_spark.columns for col in ['genres', 'startYear']):
        current_year = 2025
        df_genre_trends_uy = df_curated_uy_spark.filter(
            col('startYear').isNotNull() & 
            (col('startYear').cast(IntegerType()) >= current_year - 20) &
            col('genres').isNotNull()
        ).withColumn('startYear_int', col('startYear').cast(IntegerType())) \
         .withColumn('genre', explode(split(col('genres'), ','))) \
         .withColumn('genre', trim(col('genre')))
        
        genre_trends_uy = df_genre_trends_uy.groupBy('startYear_int', 'genre').count().orderBy('startYear_int', 'genre')
        if not genre_trends_uy.isEmpty():
            genre_trends_uy_pd = genre_trends_uy.toPandas()
            
            top_genres = genre_trends_uy_pd.groupby('genre')['count'].sum().nlargest(5).index
            genre_trends_filtered = genre_trends_uy_pd[genre_trends_uy_pd['genre'].isin(top_genres)]
            
            plt.figure(figsize=(15, 8))
            for genre in top_genres:
                genre_data = genre_trends_filtered[genre_trends_filtered['genre'] == genre]
                plt.plot(genre_data['startYear_int'], genre_data['count'], marker='o', label=genre, linewidth=2)
            
            plt.title(f"Tendencias de popularidad de géneros en uruguay (ultimos 20 años)", fontsize=16)
            plt.xlabel("Año", fontsize=12)
            plt.ylabel("Número de películas", fontsize=12)
            plt.legend(title="Género", bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(os.path.join(REPORTS_DIR, f"genre_trends_20years_{REGION_OF_INTEREST}.png"))
            plt.close()

   


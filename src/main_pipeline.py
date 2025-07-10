# src/main_pipeline.py

import os
from pyspark.sql import SparkSession
from src.config import REPORTS_DIR, LANDING_ZONE_PATH, RAW_ZONE_PATH, CURATED_ZONE_PATH
from src.data_raw import load_and_clean_to_raw_zone
from src.data_curated import transform_to_curated_zone
from src.data_interaction import generate_kpis_and_visualizations

def init_spark_session(app_name="IMDb_DataLake_Pipeline"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    return spark

def run_pipeline():

    for path in [LANDING_ZONE_PATH, RAW_ZONE_PATH, CURATED_ZONE_PATH, REPORTS_DIR]:
        os.makedirs(path, exist_ok=True)
        print(f"Directorio verificado/creado: {path}")

    spark = init_spark_session()

    try:
        
        print("\n--- Etapa 1: Cargando y limpiando datos a Raw Zone ---")
        load_and_clean_to_raw_zone(spark)


        print("\n--- Etapa 2: Transformando datos a Curated Zone ---")
        transform_to_curated_zone(spark)


        print("\n--- Etapa 3: Generando KPIs y Visualizaciones ---")
        generate_kpis_and_visualizations(spark)

        print("\nPipeline completado exitosamente.")

    except Exception as e:
        print(f"\n¡El pipeline falló! Error: {e}")
        import traceback
        traceback.print_exc() 
    finally:
        spark.stop()
        print("Sesión de Spark finalizada.")

if __name__ == "__main__":
    run_pipeline()